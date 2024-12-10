//> using dep "org.yaml:snakeyaml:1.33"
//> using dep "org.postgresql:postgresql:42.6.0"
//> using dep "org.testcontainers:postgresql:1.19.3"

import org.yaml.snakeyaml.Yaml
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.jdk.CollectionConverters._
import java.nio.file.{Files, Path, Paths}

object YamlToDB {

  /** Import YAML data into a database with predefined schema */
  def importData(yamlStr: String, conn: Connection): Unit = {
    val yaml = new Yaml()
    val data =
      yaml
        .load(yamlStr)
        .asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .toMap

    // For each table in the YAML
    data.foreach { case (tableName, rowsObj) =>
      val rows =
        rowsObj match {
          case list: java.util.List[_] =>
            list.asScala.map { row =>
              row.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
            }.toSeq
          case _ => Seq.empty[Map[String, Any]]
        }
      if (rows.nonEmpty) {
        insertRows(conn, tableName, rows)
      }
    }
  }

  /** Insert rows into a table */
  private def insertRows(
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

    val stmt = conn.prepareStatement(sql)
    try {
      rows.foreach { row =>
        bindParameters(stmt, columns, row)
        stmt.executeUpdate()
      }
    } finally {
      stmt.close()
    }
  }

  /** Bind parameters to a PreparedStatement */
  private def bindParameters(
      stmt: PreparedStatement,
      columns: Seq[String],
      row: Map[String, Any]
  ): Unit = {
    columns.zipWithIndex.foreach { case (col, i) =>
      val value = row(col)
      value match {
        case n: java.lang.Number => stmt.setInt(i + 1, n.intValue())
        case s: String if col.toLowerCase.contains("date") =>
          stmt.setDate(i + 1, java.sql.Date.valueOf(s.split(" ")(0)))
        case s: String if col.toLowerCase.contains("_at") =>
          stmt.setTimestamp(i + 1, java.sql.Timestamp.valueOf(s))
        case s: String => stmt.setString(i + 1, s)
        case null      => stmt.setNull(i + 1, java.sql.Types.VARCHAR)
        case x         => stmt.setString(i + 1, x.toString)
      }
    }
  }

  /** Export table data as CSV */
  def exportTableAsCSV(conn: Connection, tableName: String): String = {
    val stmt        = conn.createStatement()
    val rs          = stmt.executeQuery(s"SELECT * FROM $tableName ORDER BY id")
    val md          = rs.getMetaData
    val columnCount = md.getColumnCount

    // Get column names
    val header = (1 to columnCount).map(i => md.getColumnName(i)).mkString(",")

    // Get data rows
    val rows = new scala.collection.mutable.ArrayBuffer[String]()
    while (rs.next()) {
      val row = (1 to columnCount)
        .map { i =>
          val value = rs.getString(i)
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

    (header +: rows).mkString("\n")
  }

}

object Main {

  /** Detect table names from SQL schema */
  def detectTables(sql: String): Set[String] = {
    val createTablePattern = """CREATE\s+TABLE\s+(\w+)\s*\(""".r
    createTablePattern.findAllMatchIn(sql).map(_.group(1).toLowerCase).toSet
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: program <example_dir>")
      System.exit(1)
    }

    val exampleDir  = args(0)
    val initSqlPath = Paths.get(exampleDir, "init.sql")
    if (!Files.exists(initSqlPath)) {
      println(s"init.sql not found in $exampleDir")
      System.exit(1)
    }

    // Get all YAML files in the directory
    val yamlFiles = Files
      .list(Paths.get(exampleDir))
      .filter(p => p.toString.endsWith(".yaml") || p.toString.endsWith(".yml"))
      .toArray
      .map(_.asInstanceOf[Path])

    if (yamlFiles.isEmpty) {
      println(s"No YAML files found in $exampleDir")
      System.exit(1)
    }

    // Read init.sql and detect tables
    val initSql = Files.readString(initSqlPath)
    val tables  = detectTables(initSql)

    // For each YAML file
    yamlFiles.foreach { yamlFile =>
      println(s"\nProcessing ${yamlFile.getFileName}:")

      // Start a fresh database for each test
      val postgres = new PostgreSQLContainer("postgres:16")
      postgres.start()

      // Connect to database
      Class.forName("org.postgresql.Driver")
      val conn = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
      )

      try {
        // Initialize schema
        val stmt = conn.createStatement()
        stmt.execute(initSql)
        stmt.close()

        // Import data
        val yamlData = Files.readString(yamlFile)
        YamlToDB.importData(yamlData, conn)

        // Export each table as CSV
        tables.toSeq.sorted.foreach { table =>
          println(s"\n$table:")
          println(YamlToDB.exportTableAsCSV(conn, table))
        }
      } finally {
        conn.close()
        postgres.stop()
      }
    }
  }

}
