//> using file "YamlToDB.scala"
//> using dep "org.scalatest::scalatest:3.2.17"
//> using dep "org.testcontainers:postgresql:1.19.3"

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.DriverManager
import java.nio.file.{Files, Paths}

class ExamplesTest extends AnyFunSuite with Matchers {

  // Helper function to test an example directory
  def testExample(name: String): Unit = {
    test(s"example: $name") {
      val exampleDir  = s"examples/$name"
      val initSqlPath = Paths.get(exampleDir, "init.sql")
      val yamlPath    = Paths.get(exampleDir, "dataset1.yaml")

      // Verify files exist
      Files.exists(initSqlPath) shouldBe true
      Files.exists(yamlPath) shouldBe true

      // Read files
      val initSql  = Files.readString(initSqlPath)
      val yamlData = Files.readString(yamlPath)

      // Parse YAML
      val data = YamlToDB.parseYaml(yamlData)

      // Start test database
      val postgres = new PostgreSQLContainer("postgres:16")
      postgres.start()

      try {
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

          // Import data in dependency order
          sortedTables.foreach { table =>
            if (data.contains(table)) {
              YamlToDB.importDataMap(Map(table -> data(table)), conn)
            }
          }

          // Verify each table has data
          sortedTables.foreach { table =>
            val csv   = YamlToDB.exportTableAsCSV(conn, table)
            val lines = csv.split("\n")
            withClue(s"Table $table should have data:") {
              lines.length should be > 1 // Header + at least one row
            }
          }

        } finally {
          conn.close()
        }
      } finally {
        postgres.stop()
      }
    }
  }

  // Test all examples
  List(
    "books",
    "employees",
    "orders",
    "social",
    "tasks",
    "university"
  ).foreach(testExample)
}
