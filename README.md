#Neo4j Apache Spark Connector using Ne4j's Bolt protocol

Here is neo4j apache spark connector that loads data from neo4j as Spark Dataframe. I've done the implimentation using whatever knowledge of Apache spark I've. Please help me to make it better by rising bugs.

##Build
To build the connector run command ```mvn clean compile install```

##Features

It has 2 different modes to create Spark   from neo4j data.

###Using Cypher Query
For this mode user need to specify following data frame reader options
-  ```url``` bolt url
- ```user``` neo4j user name
- ```password``` neo4j password
- ```cypher``` neo4j cypher query (without limit)
- ```source``` con.spark.neo4j.connector.ds.Neo4jSource

Spark's Dataframe schema creation is done using output of the cypher query execution. Connector will throw exception in case when cypher query return empty result.
Limit should not pe present in cypher query.
Partitioning is not sppuorted in this mode.

###Using Node label
For this mode user load data for a single neo4j Node type by just providing node lable in data frame reader options. Partitioning is supported in this mode
-  ```url``` bolt url
- ```user``` neo4j user name
- ```password``` neo4j password
- ```entity``` neo4j node lable
- ```source``` con.spark.neo4j.connector.ds.Neo4jSource

code example:
```
val dataFrame = session.read
        .option("url", "bolt://localhost:7687")
        .option("user", "neo4j")
        .option("password", "password")
        .option("cypher", "match (n:Movie) return n.id,n.title,n.year")
        .format("con.spark.neo4j.connector.ds.Neo4jSource").load()
```

 Parameters for partitioning
-  ```partitionColumn``` partition column of the node
- ```upperBound``` upper bound for the partition
- ```lowerBound```  lower bound for the partition
- ```partitionsCount```  number of partition

NOTE:
 partitioning optiona and  is supported for columns of ```Numeric``` type only

code example:
```
val dataFrame = session.read.option("url", "bolt://localhost:7687")
        .option("user", "neo4j")
        .option("password", "password")
        .option("entity", "Movie")
        .option("partitionColumn", "id")
        .option("upperBound", "100")
        .option("lowerBound", "1")
        .option("partitionsCount", "4")
        .format("con.spark.neo4j.connector.ds.Neo4jSource").load()
```

###Test dataset
Load CSV file ```movies.csv```present in ```src/test/resources/test_data/movies.csv``` in Neo4j for testing

###Test Cases
please ref - ```con.spark.neo4j.connector.ds.test.TestNeo4jSource``` for test cases.






