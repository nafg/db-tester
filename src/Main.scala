import org.testcontainers.containers.PostgreSQLContainer
import java.sql.DriverManager
import java.nio.file.{Files, Path, Paths}

/** Main application */
object Main {

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

    // Read init.sql
    val initSql = Files.readString(initSqlPath)

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

        // Get table dependencies from database metadata
        val deps         = YamlToDB.detectTableDependencies(conn)
        val sortedTables = YamlToDB.sortTables(deps)

        // Import data
        val yamlData = Files.readString(yamlFile)
        val data     = YamlToDB.parseYaml(yamlData)

        // Import tables in dependency order
        sortedTables.foreach { table =>
          if (data.contains(table)) {
            YamlToDB.importDataMap(Map(table -> data(table)), conn)
          }
        }

        // Export each table as CSV
        sortedTables.foreach { table =>
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
