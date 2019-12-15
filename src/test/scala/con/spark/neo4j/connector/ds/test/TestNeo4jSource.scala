package con.spark.neo4j.connector.ds.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class TestNeo4jSource extends FunSpec with Matchers {
  val sparkConf = new SparkConf().setAppName("Neo4jSparkConnector").setMaster("local[2]")
  val session = SparkSession.builder().config(sparkConf).getOrCreate()

  describe("Spark neo4j connector") {
    it("should load data as per the specified cypher query") {
      val dataFrame = session.read
        .option("url", "bolt://localhost:7687")
        .option("user", "neo4j")
        .option("password", "password")
        .option("cypher", "match (n:Movie) return n.id,n.title,n.year")
        .format("con.spark.neo4j.connector.ds.Neo4jSource").load()

      dataFrame.printSchema()
      dataFrame.show(false)
    }

    it("should load data for a given entity with given partition details") {
      val dataFrame = session.read.option("url", "bolt://localhost:7687")
        .option("user", "neo4j")
        .option("password", "password")
        .option("entity", "Movie")
        .option("partitionColumn", "id")
        .option("upperBound", "100")
        .option("lowerBound", "1")
        .option("partitionsCount", "4")
        .format("con.spark.neo4j.connector.ds.Neo4jSource").load()

      dataFrame.printSchema()
      dataFrame.show(false)
    }
  }

}
