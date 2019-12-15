package com.spark.neo4j.connector.ds

import org.neo4j.driver.v1.{AuthTokens, Config, Driver, GraphDatabase}

case class Neo4jCredential private(url: String, user: String, password: Option[String]) {

  def this(url: String, user: String) = this(url, user, None)

  def boltConfig() = Config.build.withoutEncryption().toConfig

  def driver(): Driver = password match {
    case Some(pass) => GraphDatabase.driver(url, AuthTokens.basic(user, pass))
    case _ => GraphDatabase.driver(url, boltConfig())
  }

}

object Neo4jCredential {
  def apply(parameters: Map[String, String]): Neo4jCredential = {
    val url = getValue(parameters, "url", "url must be provided in options")
    val user = getValue(parameters, "user", "user must be provided in options")
    parameters.get("password").fold(new Neo4jCredential(url, user))(pass =>
      new Neo4jCredential(url, user, Some(pass)))
  }

  private def getValue(parameters: Map[String, String], key: String, message: String): String = {
    parameters.getOrElse(key, throw new IllegalStateException(message))
  }
}
