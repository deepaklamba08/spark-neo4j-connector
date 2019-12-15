package com.spark.neo4j.connector.ds

import org.apache.spark.sql.types._
import org.neo4j.driver.v1.{Record, Value}

import scala.collection.JavaConverters._

object TypeConverter {

  def createSchema(record: Record): StructType = {
    val fields = record.fields().asScala.map(pair =>
      sparkField(pair.key(), pair.value()))
    StructType(fields)
  }

  private def convertToSpark(fieldName: String, neoValue: Value): DataType = {
    neoValue.`type`().name().toUpperCase match {
      case "LONG" => LongType
      case "INT" => IntegerType
      case "INTEGER" => IntegerType
      case "FLOAT" => FloatType
      case "DATETIME" => DateType
      case "DATE" => DateType
      case "DOUBLE" => DoubleType
      case "NUMERIC" => FloatType
      case "STRING" => StringType
      case "BOOLEAN" => BooleanType
      case "BOOL" => BooleanType
      case "NULL" => NullType
      case "NODE" => convertNodeType(fieldName, neoValue)
      case "LIST OF ANY?" => convertListType(fieldName, neoValue)
      case "MAP" => convertMapType(fieldName, neoValue)
      case other => throw new IllegalStateException(s"type conversion not supported for - $other")
    }
  }

  private def sparkField(fieldName: String, neoValue: Value): StructField =
    StructField(fieldName, convertToSpark(fieldName, neoValue))

  private def convertNodeType(fieldName: String, neoValue: Value): DataType = {
    val entity = neoValue.asEntity()
    val values = entity.values().asScala.toList
    val keys = entity.keys().asScala.toList
    val fields = for (i <- 0 to values.size - 1) yield {
      sparkField(keys(i), values(i))
    }
    StructType(fields)
  }

  private def convertListType(fieldName: String, neoValue: Value): DataType = {
    val listValues = neoValue.asList(neo4jFunction1((v: Value) => v)).asScala
    ArrayType(convertToSpark(fieldName, listValues.head), true)
  }

  private def convertMapType(fieldName: String, neoValue: Value): DataType = {
    val mapData = neoValue.asMap(neo4jFunction1((v: Value) => v)).asScala
    val fields = mapData.map(v => StructField(v._1, convertToSpark(v._1, v._2)))
    StructType(fields.toArray)
  }

  def neo4jFunction1[K, V](f: K => V) = {
    new org.neo4j.driver.v1.util.Function[K, V]() {
      override def apply(t: K): V = f(t)
    }
  }
}
