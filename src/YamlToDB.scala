//> using dep "org.yaml:snakeyaml:1.33"
//> using dep "org.postgresql:postgresql:42.6.0"

import org.yaml.snakeyaml.Yaml
import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import scala.jdk.CollectionConverters._

/** Core data import/export functionality */
object YamlToDB {

  /** Convert YAML string to Map */
  def parseYaml(yamlStr: String): Map[String, Any] = {
    if (yamlStr.trim.isEmpty) {
      Map.empty[String, Any]
    } else {
      val yaml   = new Yaml()
      val loaded = yaml.load[Any](yamlStr)
      if (loaded == null) {
        Map.empty[String, Any]
      } else {
        loaded match {
          case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
          case _                      => Map.empty[String, Any]
        }
      }
    }
  }

  /** Convert YAML table data to sequence of rows */
  def parseTableData(tableData: Any): Seq[Map[String, Any]] = {
    tableData match {
      case list: java.util.List[_] =>
        list.asScala.map { row =>
          row.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map {
            case (k, null) => k -> null
            case (k, v)    => k -> v
          }
        }.toSeq
      case _ => Seq.empty[Map[String, Any]]
    }
  }

  /** Get column metadata for a table */
  private def getColumnTypes(conn: Connection, tableName: String): Map[String, Int] = {
    val md    = conn.getMetaData
    val rs    = md.getColumns(null, null, tableName.toLowerCase, null)
    val types = scala.collection.mutable.Map[String, Int]()
    while (rs.next()) {
      val colName  = rs.getString("COLUMN_NAME")
      val dataType = rs.getInt("DATA_TYPE")
      types(colName) = dataType
    }
    rs.close()
    types.toMap
  }

  /** Detect table dependencies from database metadata */
  def detectTableDependencies(conn: Connection): Map[String, Set[String]] = {
    val md     = conn.getMetaData
    val tables = scala.collection.mutable.Map[String, Set[String]]()

    // Get all tables first
    val rs = md.getTables(null, null, null, Array("TABLE"))
    while (rs.next()) {
      val tableName = rs.getString("TABLE_NAME").toLowerCase
      tables(tableName) = Set.empty[String]
    }
    rs.close()

    // Then get foreign keys for each table
    tables.keys.foreach { tableName =>
      val fkRs = md.getImportedKeys(null, null, tableName)
      val refs = scala.collection.mutable.Set[String]()
      while (fkRs.next()) {
        val pkTable = fkRs.getString("PKTABLE_NAME").toLowerCase
        refs += pkTable
      }
      fkRs.close()
      tables(tableName) = refs.toSet
    }

    tables.toMap
  }

  /** Sort tables in dependency order */
  def sortTables(deps: Map[String, Set[String]]): Seq[String] = {
    // Find strongly connected components (cycles)
    def findCycles(
        node: String,
        visited: Set[String],
        stack: Set[String],
        cycles: Set[Set[String]]
    ): Set[Set[String]] = {
      if (stack.contains(node)) {
        // Found a cycle
        val cycle = stack.dropWhile(_ != node)
        cycles + cycle
      } else if (visited.contains(node)) {
        cycles
      } else {
        val newStack = stack + node
        deps.getOrElse(node, Set.empty).foldLeft(cycles) { (acc, next) =>
          findCycles(next, visited + node, newStack, acc)
        }
      }
    }

    val cycles =
      deps.keys.foldLeft(Set.empty[Set[String]]) { (acc, node) =>
        findCycles(node, Set.empty, Set.empty, acc)
      }

    // Create a DAG by collapsing cycles into single nodes
    val cycleSets = cycles.toSeq
    val cycleMap  = cycleSets.flatMap(cycle => cycle.map(_ -> cycle)).toMap

    // Merge dependencies for cycles
    val mergedDeps = deps.map { case (table, refs) =>
      val cycle = cycleMap.get(table)
      val mergedRefs = refs.flatMap { ref =>
        cycleMap.get(ref) match {
          case Some(refCycle) if cycle.contains(refCycle) => None // Skip internal cycle refs
          case Some(refCycle) => Some(refCycle.head) // Use cycle representative
          case None           => Some(ref)
        }
      }
      table -> mergedRefs
    }

    // Sort non-cyclic tables
    var result    = Seq.empty[String]
    var remaining = mergedDeps
    var lastSize  = -1

    while (remaining.nonEmpty && lastSize != remaining.size) {
      lastSize = remaining.size
      val (ready, notReady) = remaining.partition { case (table, refs) =>
        val cycle = cycleMap.get(table)
        refs.forall { ref =>
          !remaining.contains(ref) ||   // Already processed
          cycle.exists(_.contains(ref)) // Same cycle
        }
      }
      val readyTables = ready.keys.toSeq.sorted

      // For each ready table, add all tables in its cycle
      val expanded = readyTables.flatMap { table =>
        cycleMap.get(table) match {
          case Some(cycle) => cycle.toSeq.sorted
          case None        => Seq(table)
        }
      }
      result = result ++ expanded
      remaining = notReady
    }

    // Force-add remaining tables
    if (remaining.nonEmpty) {
      val expanded = remaining.keys.toSeq.sorted.flatMap { table =>
        cycleMap.get(table) match {
          case Some(cycle) => cycle.toSeq.sorted
          case None        => Seq(table)
        }
      }
      result = result ++ expanded
    }

    // Ensure all tables are included
    val missing = deps.keys.toSet -- result.toSet
    if (missing.nonEmpty) {
      result = result ++ missing.toSeq.sorted
    }

    result
  }

  /** Bind a single parameter to a PreparedStatement */
  def bindParameter(
      stmt: PreparedStatement,
      columnName: String,
      value: Any,
      sqlType: Int,
      index: Int
  ): Unit = {
    value match {
      case null | "null" | "" => stmt.setNull(index, sqlType)
      case n: java.lang.Number =>
        sqlType match {
          case Types.INTEGER | Types.SMALLINT | Types.TINYINT => stmt.setInt(index, n.intValue())
          case Types.BIGINT                                   => stmt.setLong(index, n.longValue())
          case Types.DECIMAL | Types.NUMERIC =>
            stmt.setBigDecimal(index, java.math.BigDecimal.valueOf(n.doubleValue()))
          case Types.FLOAT | Types.REAL => stmt.setFloat(index, n.floatValue())
          case Types.DOUBLE             => stmt.setDouble(index, n.doubleValue())
          case _                        => stmt.setString(index, n.toString)
        }
      case s: String if s.toLowerCase == "null" => stmt.setNull(index, sqlType)
      case s: String =>
        sqlType match {
          case Types.INTEGER | Types.SMALLINT | Types.TINYINT =>
            try {
              stmt.setInt(index, s.toInt)
            } catch {
              case _: NumberFormatException => stmt.setNull(index, sqlType)
            }
          case Types.BIGINT =>
            try {
              stmt.setLong(index, s.toLong)
            } catch {
              case _: NumberFormatException => stmt.setNull(index, sqlType)
            }
          case Types.DECIMAL | Types.NUMERIC =>
            try {
              stmt.setBigDecimal(index, new java.math.BigDecimal(s))
            } catch {
              case _: NumberFormatException => stmt.setNull(index, sqlType)
            }
          case Types.FLOAT | Types.REAL =>
            try {
              stmt.setFloat(index, s.toFloat)
            } catch {
              case _: NumberFormatException => stmt.setNull(index, sqlType)
            }
          case Types.DOUBLE =>
            try {
              stmt.setDouble(index, s.toDouble)
            } catch {
              case _: NumberFormatException => stmt.setNull(index, sqlType)
            }
          case Types.DATE      => stmt.setDate(index, java.sql.Date.valueOf(s.split(" ")(0)))
          case Types.TIMESTAMP => stmt.setTimestamp(index, java.sql.Timestamp.valueOf(s))
          case _               => stmt.setString(index, s)
        }
      case b: java.lang.Boolean => stmt.setBoolean(index, b)
      case x                    => stmt.setString(index, x.toString)
    }
  }

  /** Insert rows into a table */
  def insertRows(
      conn: Connection,
      tableName: String,
      rows: Seq[Map[String, Any]]
  ): Unit = {
    if (rows.isEmpty)
      return

    val columns      = rows.head.keys.toSeq
    val colList      = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")
    val sql          = s"INSERT INTO $tableName ($colList) VALUES ($placeholders)"

    val columnTypes = getColumnTypes(conn, tableName)
    val stmt        = conn.prepareStatement(sql)
    try {
      rows.foreach { row =>
        columns.zipWithIndex.foreach { case (col, i) =>
          bindParameter(stmt, col, row(col), columnTypes(col), i + 1)
        }
        stmt.executeUpdate()
      }
    } finally {
      stmt.close()
    }
  }

  /** Export table data as CSV */
  def exportTableAsCSV(conn: Connection, tableName: String): String = {
    val stmt = conn.createStatement()

    // Get column names first
    val md      = conn.getMetaData
    val rs      = md.getColumns(null, null, tableName, null)
    val columns = new scala.collection.mutable.ArrayBuffer[String]()
    while (rs.next()) {
      columns += rs.getString("COLUMN_NAME")
    }
    rs.close()

    if (columns.isEmpty) {
      throw new RuntimeException(s"No columns found for table $tableName")
    }

    // Query data
    val columnList = columns.mkString(", ")
    val orderBy =
      if (columns.contains("id"))
        "ORDER BY id"
      else
        ""
    val dataRs = stmt.executeQuery(s"SELECT $columnList FROM $tableName $orderBy")

    // Get data rows
    val rows = new scala.collection.mutable.ArrayBuffer[String]()
    while (dataRs.next()) {
      val row = columns
        .map { col =>
          val value = dataRs.getString(col)
          if (value == null)
            ""
          else if (value.contains(","))
            s""""$value""""
          else
            value
        }
        .mkString(",")
      rows += row
    }

    (columns.mkString(",") +: rows).mkString("\n")
  }

  /** Import data from a Map */
  def importDataMap(data: Map[String, Any], conn: Connection): Unit = {
    // For each table in the data
    data.foreach { case (tableName, rowsObj) =>
      val rows = parseTableData(rowsObj)

      // For self-referential tables, sort rows by dependencies
      val selfRefs = rows.exists(_.values.exists {
        case n: java.lang.Number => rows.exists(_.get("id").exists(_.toString == n.toString))
        case _                   => false
      })

      if (selfRefs) {
        // Sort rows so referenced rows come first
        val (independent, dependent) = rows.partition(row =>
          !row.values.exists {
            case n: java.lang.Number => rows.exists(_.get("id").exists(_.toString == n.toString))
            case _                   => false
          }
        )
        if (independent.nonEmpty) {
          insertRows(conn, tableName, independent)
        }
        if (dependent.nonEmpty) {
          insertRows(conn, tableName, dependent)
        }
      } else if (rows.nonEmpty) {
        insertRows(conn, tableName, rows)
      }
    }
  }

  /** Import YAML data into a database with predefined schema */
  def importData(yamlStr: String, conn: Connection): Unit = {
    val data = parseYaml(yamlStr)
    importDataMap(data, conn)
  }

}
