package con.spark.neo4j.connector.ds

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.{NumericType, StructField, StructType}
import org.neo4j.driver.v1.{Session, Transaction, TransactionWork}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case class Neo4jOptions private(parameters: CaseInsensitiveMap[String]) {

  val url = parameters("url")

  val cypher = parameters.get("cypher")

  val entity = parameters.get("entity")

  val partitionColumn = parameters.get("partitionColumn")
  val upperBound = parameters.get("upperBound")
  val lowerBound = parameters.get("lowerBound")
  val partitionsCount = parameters.get("partitionsCount")

  def createSchema(neo4jCredential: Neo4jCredential): StructType = {
    (cypher, entity) match {
      case (Some(_), Some(_)) => throw new IllegalStateException("query and entity can not be present at the same time")
      case (Some(cypherQuery), None) => buildSchema(cypherQuery, neo4jCredential, createSchemaFromCypher)
      case (None, Some(entityName)) => buildSchema(entityName, neo4jCredential, createSchemaFromNode)
      case (None, None) => throw new IllegalStateException("Either query and entity must be present")
    }
  }

  private def buildSchema(cypher: String, neo4jCredential: Neo4jCredential, fun: (String, Session) => StructType): StructType = {
    val driver = neo4jCredential.driver()
    val session = driver.session()
    Try(fun(cypher, session)) match {
      case Success(schema) =>
        session.close()
        driver.close()
        schema
      case Failure(exception) => throw exception
    }
  }

  private def createSchemaFromCypher(cypher: String, session: Session): StructType = {

    val runTransaction = new TransactionWork[Either[String, StructType]]() {
      override def execute(transaction: Transaction): Either[String, StructType] = {
        val result = transaction.run(cypher)
        if (!result.hasNext) Left("Result must not be empty to determine schema")
        else Right(TypeConverter.createSchema(result.peek()))
      }
    }
    session.readTransaction(runTransaction) match {
      case Left(message) => throw new IllegalStateException(message)
      case Right(schema) => schema
    }
  }

  private def createSchemaFromNode(node: String, session: Session) = {
    val cypher = s"match (n:$node) return n limit 1"
    createSchemaFromCypher(cypher, session)
  }

  private def getCypherQuery = {
    (cypher, entity) match {
      case (Some(_), Some(_)) => throw new IllegalStateException("query and entity can not be present at the same time")
      case (Some(cypherQuery), None) => cypherQuery
      case (None, Some(entityName)) => s"match (n:$entityName) return n"
      case (None, None) => throw new IllegalStateException("Either query and entity must be present")
    }
  }

  def createPartitions(schema: StructType): Array[Partition] = {
    if (partitionColumn.isEmpty) {
      validate(lowerBound.isEmpty && upperBound.isEmpty && partitionsCount.isEmpty, "lower and upper bounds and partitions count must be empty when partitionColumn is not present")
      Array(Neo4jPartition(getCypherQuery, 0))
    } else {
      validate(lowerBound.nonEmpty && upperBound.nonEmpty && partitionsCount.nonEmpty, "when partition count is present then lower and upper bounds must be present")
      val headField = schema.fields.head.dataType.asInstanceOf[StructType]
      headField.fields.find(_.name.equals(partitionColumn.get)).fold(throw new IllegalStateException(s"Partition column must be present in schema "))(column => {
        column.dataType match {
          case _: NumericType => this.createColumnPartitionV2(column)
          case other => throw new IllegalStateException(s"Partition not supported for column type $other")
        }
      })
    }
  }

  private def toInt(value: Option[String]): Int = value.fold(throw new IllegalStateException("value must be present"))(_.toInt)

  private def validate(expression: Boolean, message: String) = {
    if (!expression) {
      throw new IllegalStateException(message)
    }
  }

  private def createColumnPartitionV2(column: StructField): Array[Partition] = {
    val lowerB = toInt(lowerBound)
    val upperB = toInt(upperBound)
    val parts = toInt(partitionsCount)

    val numPartitions =
      if ((upperB - lowerB) >= parts || (upperB - lowerB) < 0) {
        parts
      } else {
        upperB - lowerB
      }

    val stepSize = (upperB - lowerB) / numPartitions
    val columnName = s"n.${column.name}"
    val (_, _, _, p) = (0 until numPartitions).foldLeft[(Int, Int, Int, ArrayBuffer[Partition])]((0, lowerB, lowerB + stepSize, new ArrayBuffer[Partition]()))((acc, index) => {
      val (index, currentValue, nextValue, partitions) = (acc._1, acc._2, acc._3, acc._4)
      val whereClause = if (index == numPartitions - 1 && currentValue != upperB)
        s"$columnName >= $currentValue AND $columnName < $upperB"
      else s"$columnName >= $currentValue AND $columnName < $nextValue"
      val cypherQuery = s"match (n:${entity.get}) where $whereClause return n"
      (index + 1, nextValue, nextValue + stepSize, partitions += Neo4jPartition(cypherQuery, index))
    })
    p.toArray
  }

}

object Neo4jOptions {
  def apply(parameters: Map[String, String]): Neo4jOptions = new Neo4jOptions(CaseInsensitiveMap(parameters))
}