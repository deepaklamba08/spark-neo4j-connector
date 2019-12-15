package com.spark.neo4j.connector.ds

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.neo4j.driver.v1.util.Pair
import org.neo4j.driver.v1.{Session, Transaction, TransactionWork, Value}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Neo4jRDD(sc: SparkContext,
               partitions: Array[Partition],
               requiredColumns: Array[String],
               schema: StructType,
               neoCred: Neo4jCredential,
               neoOpts: Neo4jOptions) extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] = partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val driver = neoCred.driver()
    val session = driver.session()

    val schemaMap = schema.fields.map(f => (f.name, f)).toMap
    Try(readData(session, schemaMap, split.asInstanceOf[Neo4jPartition])) match {
      case Success(value) =>
        session.close()
        driver.close()
        value
      case Failure(exception) => throw exception
    }
  }

  private def readData(session: Session, schemaMap: Map[String, StructField], split: Neo4jPartition): Iterator[Row] = {
    val runTransaction = new TransactionWork[Iterator[Row]]() {
      override def execute(transaction: Transaction): Iterator[Row] = {
        val result = transaction.run(split.cypher)
        val dataMap: Seq[Map[String, Value]] = result.list().asScala.map(r => toMap(r.fields()))
        dataMap.map(record => toInternalRow(schemaMap, record)).iterator
      }
    }
    session.readTransaction(runTransaction)
  }

  private def toMap(data: util.List[Pair[String, Value]]): Map[String, Value] = {
    data.asScala.map(p => (p.key(), p.value())).toMap
  }

  private def toInternalRow(schemaMap: Map[String, StructField], data: Map[String, Value]): Row = {
    val rowValues = requiredColumns.map(f => schemaMap(f)).map(f => convertV2(f.dataType, data(f.name)))
    Row.fromSeq(rowValues)
  }

  private def convertV2(dataType: DataType, value: Value): Any = {
    dataType match {
      case _: StringType => value.asString()
      case _: IntegerType => value.asInt()
      case _: BooleanType => value.asBoolean()
      case array: ArrayType => value.values().asScala.map(v => convertV2(array.elementType, v))
      case struct: StructType =>
        val dataMap = value.asMap(TypeConverter.neo4jFunction1((n: Value) => n)).asScala
        val values = struct.fields.map(f => convertV2(f.dataType, dataMap(f.name)))
        Row.fromSeq(values.toSeq)
      case other => throw new IllegalStateException(s"type conversion not supported for - $other")
    }
  }

}
