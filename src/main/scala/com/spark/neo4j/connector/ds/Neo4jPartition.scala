package com.spark.neo4j.connector.ds

import org.apache.spark.Partition

case class Neo4jPartition(val cypher: String, override val index: Int) extends Partition {

}
