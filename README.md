# Azure CosmosDB Spark Connector [![Build Status](https://travis-ci.org/Azure/azure-cosmosdb-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-cosmosdb-spark)

The official connector for [Azure CosmosDB](http://cosmosdb.com) and [Apache Spark](http://spark.apache.org). 

Official instructions for using the connector are included in the Cosmos DB documentation, in the [Accelerate real-time big-data analytics with the Spark to Cosmos DB connector](https://docs.microsoft.com/azure/documentdb/documentdb-spark-connector) article.

This project provides a client library that allows Azure Cosmos DB to act as an input source or output sink for Spark jobs.

Officially supports Spark version: 2.0.2/2.1.0/2.2.0, Scala version: 2.10/2.11, Azure DocumentDB Java SDK: 1.15.0

There are currently two approaches to connect Apache Spark to Azure Cosmos DB:

* Using `pyDocumentDB`
* Using `azure-cosmosdb-spark` - a Java-based Spark to Cosmos DB connector based utilizing the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java)

See the [user guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-DocumentDB-Spark-Connector-User-Guide) for more information about the API.

## Requirements

* Apache Spark 2.0.2+
* Java Version >= 7.0
* If using Python
  * `pyDocumentDB` package
  * Python >= 2.7 or Python >= 3.3
* If using Scala
  * Azure DocumentDB Java SDK 1.15.0

For those using HDInsight, this has been tested on HDI 3.5


## How to connect Spark to Cosmos DB using pyDocumentDB

The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `Cosmos DB`. Here's a small code snippet that queries for airport codes from the DoctorWho Azure Cosmos DB database; the results are in the `df` DataFrame.

```python
# Import Necessary Libraries
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

# Configuring the connection policy (allowing for endpoint discovery)
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["Central US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]

# Set keys to connect to Cosmos DB 
masterKey = 'SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==' 
host = 'https://doctorwho.documents.azure.com:443/'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

# Configure Database and Collections
databaseId = 'airports'
collectionId = 'codes'

# Configurations the Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId

# Set query parameter
querystr = "SELECT c.City FROM c WHERE c.State='WA'"

# Query documents
query = client.QueryDocuments(collLink, querystr, options=None, partition_key=None)

# Query for partitioned collections
# query = client.QueryDocuments(collLink, querystr, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# Push into list `elements`
elements = list(query)

# Create `df` Spark DataFrame from `elements` Python list
df = spark.createDataFrame(elements)
```

## How to connect Spark to Cosmos DB using azure-cosmosdb-spark

The `azure-cosmosdb-spark` connector connects Apache Spark to Cosmos DB using the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java).  Here's a small code snippet that queries for flight data from the DoctorWho Azure Cosmos DB database; the results are in the `df` DataFrame.

### Python
```python
# Base Configuration
flightsConfig = {
"Endpoint" : "https://doctorwho.documents.azure.com:443/",
"Masterkey" : "SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==",
"Database" : "DepartureDelays",
"preferredRegions" : "Central US;East US2",
"Collection" : "flights_pcoll", 
"SamplingRatio" : "1.0",
"schema_samplesize" : "1000",
"query_pagesize" : "2147483647",
"query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c"
}

# Connect via Spark connector to create Spark DataFrame
flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**flightsConfig).load()
flights.count()

# Queries
flights.createOrReplaceTempView("c")
seaflights = spark.sql("SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'")

# Write configuration
writeConfig = {
"Endpoint" : "https://doctorwho.documents.azure.com:443/",
"Masterkey" : "SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==",
"Database" : "DepartureDelays",
"Collection" : "flights_fromsea",
"Upsert" : "true"
}
seaflights.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()

```


### Scala
```scala
// Import Necessary Libraries
import org.joda.time._
import org.joda.time.format._

// Earlier versions of the connector
// import com.microsoft.azure.documentdb.spark.schema._
// import com.microsoft.azure.documentdb.spark._
// import com.microsoft.azure.documentdb.spark.config.Config

// Current version of the connector
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val readConfig2 = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==",
"Database" -> "DepartureDelays",
"PreferredRegions" -> "Central US;East US2;",
"Collection" -> "flights_pcoll", 
"SamplingRatio" -> "1.0"))

// Create collection connection 
// Earlier version of the connector
// val coll = spark.sqlContext.read.DocumentDB(readConfig2)

// Current version of the connector
val coll = spark.sqlContext.read.cosmosDB(readConfig2)
coll.createOrReplaceTempView("c")

// Queries
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
val df = spark.sql(query)

// Run DF query (count)
df.count()

// Configure connection to the sink collection
val writeConfig = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==",
"Database" -> "DepartureDelays",
"PreferredRegions" -> "Central US;East US2;",
"Collection" -> "flights_fromsea",
"WritingBatchSize" -> "100"))

// Write the dataframe 
df.write.cosmosDB(writeConfig)

// Upsert the dataframe
import org.apache.spark.sql.SaveMode
df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)

// Alternatively, write from an RDD
// df.rdd.saveToCosmosDB(writeConfig)
```


## How to build the connector
Currently, this connector project uses `maven` so to build without dependencies, you can run:

```sh
mvn clean package
```

## Download the connector
You can also download the latest versions of the jar within the [releases](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases) folder.

The current version of the Spark connector is [azure-cosmosdb-spark_1.0.0_2.1.0_2.11](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-1.0.0):
* `azure-cosmosdb-spark` version: 1.0.0
* Apache Spark version: 2.1.0
* Scala version: 2.11

## Download from Maven
You can also download the JARs from maven:

| Spark | Scala | Latest version |
|---|---|---|
| 2.2.0 | 2.11 | [azure-cosmosdb-spark_1.0.0-2.2.0_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.2.0_2.11/1.0.0) |
| 2.2.0 | 2.10 | [azure-cosmosdb-spark_1.0.0-2.2.0_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.2.0_2.10/1.0.0) |
| 2.1.0 | 2.11 | [azure-cosmosdb-spark_1.0.0-2.1.0_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.1.0_2.11/1.0.0) |
| 2.1.0 | 2.10 | [azure-cosmosdb-spark_1.0.0-2.1.0_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.1.0_2.10/1.0.0) |
| 2.0.2 | 2.11 | [azure-cosmosdb-spark_0.0.3-2.0.2_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.0.2_2.11/0.0.3) |
| 2.0.2 | 2.10 | [azure-cosmosdb-spark_0.0.3-2.0.2_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.0.2_2.10/0.0.3) |
