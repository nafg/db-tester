import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.{DriverManager, Types}

/** Unit tests */
class YamlToDBTest extends AnyFunSuite with Matchers {
  test("parseYaml should handle empty YAML") {
    val yaml   = ""
    val result = YamlToDB.parseYaml(yaml)
    result shouldBe Map.empty[String, Any]
  }

  test("parseYaml should handle null") {
    val yaml   = "null"
    val result = YamlToDB.parseYaml(yaml)
    result shouldBe Map.empty[String, Any]
  }

  test("parseYaml should handle non-map YAML") {
    val yaml   = "- item1\n- item2"
    val result = YamlToDB.parseYaml(yaml)
    result shouldBe Map.empty[String, Any]
  }

  test("parseYaml should handle simple map") {
    val yaml   = "key: value"
    val result = YamlToDB.parseYaml(yaml)
    result shouldBe Map("key" -> "value")
  }

  test("parseYaml should handle nested map") {
    val yaml =
      """
      |outer:
      |  inner: value
      |""".stripMargin
    val result = YamlToDB.parseYaml(yaml)
    result should have size 1
    result("outer").asInstanceOf[java.util.Map[String, Any]].get("inner") shouldBe "value"
  }

  test("parseYaml should handle table data") {
    val yaml =
      """
      |table1:
      |  - id: 1
      |    name: "Test"
      |""".stripMargin.trim
    val result = YamlToDB.parseYaml(yaml)
    result should have size 1
    result should contain key "table1"
    val tableData = result("table1").asInstanceOf[java.util.List[?]]
    tableData should have size 1
    val row = tableData.get(0).asInstanceOf[java.util.Map[String, Any]]
    row.get("id") shouldBe 1
    row.get("name") shouldBe "Test"
  }

  test("detectTableDependencies should find dependencies") {
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
        // Create test schema
        val sql =
          """
          |CREATE TABLE parent (
          |  id SERIAL PRIMARY KEY
          |);
          |CREATE TABLE child (
          |  id SERIAL PRIMARY KEY,
          |  parent_id INTEGER REFERENCES parent(id)
          |);
          |""".stripMargin

        val stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()

        // Test dependencies
        val deps = YamlToDB.detectTableDependencies(conn)
        deps("child") should contain("parent")
        deps("parent") shouldBe empty
      } finally {
        conn.close()
      }
    } finally {
      postgres.stop()
    }
  }

  test("sortTables should handle cycles") {
    val deps = Map(
      "a" -> Set("b"),
      "b" -> Set("c"),
      "c" -> Set("a")
    )
    val sorted = YamlToDB.sortTables(deps)
    sorted should contain allOf ("a", "b", "c")
  }

  test("parseTableData should handle empty data") {
    val data   = null
    val result = YamlToDB.parseTableData(data)
    result shouldBe empty
  }

  test("parseTableData should parse list of maps") {
    import scala.jdk.CollectionConverters._
    val data = new java.util.ArrayList[java.util.Map[String, Any]]()
    val row  = new java.util.HashMap[String, Any]()
    row.put("id", 1)
    row.put("name", "Test")
    data.add(row)

    val result = YamlToDB.parseTableData(data)
    result should have size 1
    result.head should contain allOf (
      "id"   -> 1,
      "name" -> "Test"
    )
  }
}
