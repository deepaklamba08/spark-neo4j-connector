package com.spark.neo4j.connector.ds

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

class Neo4jSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val neoCred = Neo4jCredential(parameters)
    val neoOpts = Neo4jOptions(parameters)
    val schema = neoOpts.createSchema(neoCred)
    val partitions = neoOpts.createPartitions(schema)
    Neo4jRelation(sqlContext, schema, neoCred, neoOpts,partitions)
  }
}

case class Neo4jRelation(override val sqlContext: SQLContext,
                         override val schema: StructType,
                         neoCred: Neo4jCredential,
                         neoOpts: Neo4jOptions,
                         partitions: Array[Partition]
                        ) extends BaseRelation with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new Neo4jRDD(sqlContext.sparkContext,partitions, requiredColumns, schema, neoCred, neoOpts).asInstanceOf[RDD[Row]]
  }
}


