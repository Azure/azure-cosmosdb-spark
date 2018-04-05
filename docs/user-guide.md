<img src="https://raw.githubusercontent.com/dennyglee/azure-cosmosdb-spark/master/docs/images/azure-cosmos-db-icon.png" width="75">  &nbsp; Azure Cosmos DB Connector for Apache Spark
==========================================
# User Guide


Current Version of Spark Connector: 1.1.0 | [![Build Status](https://travis-ci.org/Azure/azure-cosmosdb-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-cosmosdb-spark)



`azure-cosmosdb-spark` is the official connector for [Azure CosmosDB](http://cosmosdb.com) and [Apache Spark](http://spark.apache.org). The connector allows you to easily read to and write from Azure Cosmos DB via Apache Spark DataFrames in `python` and `scala`.  It also allows you to easily create a lambda architecture for batch-processing, stream-processing, and a serving layer while being globally replicated and minimizing the latency involved in working with big data. 


<details>
<summary><strong><em>Table of Contents</em></strong></summary>

* [Introduction](#Introduction)
* [Requirements](#Requirements)
* [Working with the connector](#working-with-the-connector)
  * [Using spark-cli](#using-spark-cli)
  * [Using Jupyter notebooks](#using-jupyter-notebooks)
  * [Using Databricks notebooks](#using-databricks-notebooks)
  * [Build the connector](#build-the-connector)
* [Reading from Cosmos dB](#Reading-from-Cosmos-DB)
  * Python
    * Show adding to map
    * Reading Batch
  * Scala
    * Show adding to map
    * Reading Batch
    * Reading Change Feed
  * Predicate pushdown
  * To cache or not to cache, that is the question
* [Writing to Cosmos DB](#Writing-to-Cosmos-DB)
  * Python
    * Show adding to map
  * Scala
    * Show adding to map
  * Examples
* [Structured Streaming](#Structured-Streaming)
  * TTL
  * Reading Change Feed
    * Python
    * Scala
* [Lambda Architecture](#Lambda-Architecture)
  * Point to Lambda Architecture
* [Configuration Reference](#Configuration-Reference)
    * Parmeters
    * Reading
    * Writing

</details>


## Introduction

`azure-cosmosdb-spark` is the official connector for [Azure CosmosDB](http://cosmosdb.com) and [Apache Spark](http://spark.apache.org). The connector allows you to easily read to and write from Azure Cosmos DB via Apache Spark DataFrames in `python` and `scala`.  It also allows you to easily create a lambda architecture for batch-processing, stream-processing, and a serving layer while being globally replicated and minimizing the latency involved in working with big data. 

The connector utilizes the [Azure Cosmos DB Java SDK](https://github.com/Azure/azure-documentdb-java) via following data flow:

![](https://github.com/Azure/azure-documentdb-spark/blob/master/docs/images/Azure-DocumentDB-Spark_Connector.png)

The data flow is as follows:

1. Connection is made from Spark driver node to Cosmos DB gateway node to obtain the partition map.  Note, user only specifies Spark and Cosmos DB connections, the fact that it connects to the respective master and gateway nodes is transparent to the user.
2. This information is provided back to the Spark master node.  At this point, we should be able to parse the query to determine which partitions (and their locations) within Cosmos DB we need to access.
3. This information is transmitted to the Spark worker nodes ...
4. Thus allowing the Spark worker nodes to connect directly to the Cosmos DB partitions directly to extract the data that is needed and bring the data back to the Spark partitions within the Spark worker nodes.

The important call out is that communication between Spark and Cosmos DB is significantly faster because the data movement is between the Spark worker nodes and the Cosmos DB data nodes (partitions).




### Building the Azure Cosmos DB Spark Connector
Currently, this connector project uses maven so to build without dependencies, you can run:
```
mvn clean package
```
You can also download the latest versions of the jar within the [releases](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases) folder.  The current version of the Spark connector which you can download directly is [azure-cosmosdb-spark_1.0.0_2.1.0_2.11](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-1.0.0).

### Including the Azure DocumentDB Spark JAR
Prior to executing any code, you will first need to include the Azure DocumentDB Spark JAR.  If you are using the `spark-shell`, then you can include the JAR using the `--jars` option.  

```
spark-shell --master yarn --jars /$location/azure-cosmosdb-spark_2.1.0_2.11-1.0.0-uber.jar
```

or if you want to execute the jar without dependencies:

```
spark-shell --master yarn --jars /$location/azure-cosmosdb-spark_2.1.0_2.11-1.0.0.jar,/$location/azure-documentdb-1.14.0.jar,/$location/azure-documentdb-rx-0.9.0-rc2.jar,/$location/json-20140107.jar,/$location/rxjava-1.3.0.jar,/$location/rxnetty-0.4.20.jar 
```

If you are using a notebook service such as Azure HDInsight Jupyter notebook service, you can use the `spark magic` commands:

```
%%configure
{ "name":"Spark-to-Cosmos_DB_Connector", 
  "jars": ["wasb:///example/jars/1.0.0/azure-cosmosdb-spark_2.1.0_2.11-1.0.0.jar", "wasb:///example/jars/1.0.0/azure-documentdb-1.14.0.jar", "wasb:///example/jars/1.0.0/azure-documentdb-rx-0.9.0-rc2.jar", "wasb:///example/jars/1.0.0/json-20140107.jar", "wasb:///example/jars/1.0.0/rxjava-1.3.0.jar", "wasb:///example/jars/1.0.0/rxnetty-0.4.20.jar"],
  "conf": {
    "spark.jars.excludes": "org.scala-lang:scala-reflect"
   }
}
```

The `jars` command allows you to include the two jars needed for `azure-cosmosdb-spark` (itself and the Azure DocumentDB Java SDK) and excludes `scala-reflect` so it does not interfere with the Livy calls made (Jupyter notebook > Livy > Spark).


### Connecting Spark to Cosmos DB via the azure-cosmosdb-spark
While the communication transport is a little more complicated, executing a query from Spark to Cosmos DB using `azure-cosmosdb-spark` is significantly faster.

Below is a code snippet on how to use `azure-cosmosdb-spark` within a Spark context.

#### Python
```python
# Base Configuration
flightsConfig = {
"Endpoint" : "https://doctorwho.documents.azure.com:443/",
"Masterkey" : "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==",
"Database" : "DepartureDelays",
"preferredRegions" : "Central US;East US2",
"Collection" : "flights_pcoll", 
"SamplingRatio" : "1.0",
"schema_samplesize" : "1000",
"query_pagesize" : "2147483647",
"query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
}

# Connect via Spark connector to create Spark DataFrame
flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**flightsConfig).load()
flights.count()
```

#### Scala
```scala
// Import Necessary Libraries
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val readConfig2 = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==",
"Database" -> "DepartureDelays",
"preferredRegions" -> "Central US;East US2;",
"Collection" -> "flights_pcoll", 
"query_custom" -> "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
"SamplingRatio" -> "1.0"))
 
// Create collection connection 
val coll = spark.sqlContext.read.cosmosDB(readConfig2)
coll.createOrReplaceTempView("c")
```

As noted in the code snippet:

- `azure-cosmosdb-spark` contains the all the necessary connection parameters including the preferred locations (i.e. choosing which read replica in what priority order).
- Just import the necessary libraries and configure your masterKey and host to create the Cosmos DB client.

### Executing Spark Queries via azure-cosmosdb-spark

Below is an example using the above Cosmos DB instance via the specified read-only keys. This code snippet below connects to the DepartureDelays.flights_pcoll collection (in the DoctorWho account as specified earlier) running a query to extract the flight delay information of flights departing from Seattle.

```
// Queries
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.destination = 'SFO'"
val df = spark.sql(query)

// Run DF query (count)
df.count()

// Run DF query (show)
df.show()
```

### Scenarios

Connecting Spark to Cosmos DB using `azure-cosmosdb-spark` are typically for scenarios where:

* You want to use Python and/or Scala
* You have a large amount of data to transfer between Apache Spark and Cosmos DB


To give you an idea of the query performance difference, please refer to [Query Test Runs](https://github.com/Azure/azure-documentdb-spark/wiki/Query-Test-Runs) in this wiki.