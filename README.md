# Azure CosmosDB Spark Connector [![Build Status](https://travis-ci.org/Azure/azure-cosmosdb-spark.svg?branch=master)](https://travis-ci.org/Azure/azure-cosmosdb-spark)

The official connector for [Azure CosmosDB](http://cosmosdb.com) and [Apache Spark](http://spark.apache.org). Please note, the official stable instructions for using the connector are included in the Cosmos DB documentation, in the [Accelerate real-time big-data analytics with the Spark to Cosmos DB connector](https://docs.microsoft.com/azure/documentdb/documentdb-spark-connector) article.

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
  * Azure Documentdb RX 0.9.0

For those using HDInsight, this has been tested on HDI 3.5 and 3.6


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

As noted in the [user guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-DocumentDB-Spark-Connector-User-Guide), while using `pyDocumentDB` may be easier to configure, it is comparatively slower to the `azure-cosmosdb-spark` connector.  For larger queries, you would get faster performance by using the `azure-cosmosdb-spark` connector below.


## How to connect Spark to Cosmos DB using azure-cosmosdb-spark

The `azure-cosmosdb-spark` connector connects Apache Spark to Cosmos DB using the [Azure Cosmos DB Java SDK](https://github.com/Azure/azure-documentdb-java).  Here's a small code snippet that queries for flight data from the DoctorWho Azure Cosmos DB database; the results are in the `df` DataFrame.

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
"query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
}

# Connect via Spark connector to create Spark DataFrame
flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**flightsConfig).load()
flights.count()

# Queries
flights.createOrReplaceTempView("c")
seaflights = spark.sql("SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.destination = 'SFO'")

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

// Current version of the connector
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// Configure connection to your collection
val baseConfig = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "SPSVkSfA7f6vMgMvnYdzc1MaWb65v4VQNcI2Tp1WfSP2vtgmAwGXEPcxoYra5QBHHyjDGYuHKSkguHIz1vvmWQ==",
"Database" -> "DepartureDelays",
"PreferredRegions" -> "Central US;East US2;",
"Collection" -> "flights_pcoll", 
"SamplingRatio" -> "1.0",
"query_custom" -> "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"))

// Current version of the connector
val coll = spark.sqlContext.read.cosmosDB(baseConfig)
coll.createOrReplaceTempView("c")

// Queries
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.destination = 'SFO'"
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

### Using a Jupyter Notebook
If using a notebook like Jupyter, you can configure the jar dependencies as shown below, which gives an example of pulling jars from a default Blob Storage account (i.e., if using HDInsight) and also pulling from Maven.

```scala
%%configure -f
{
    "executorMemory": "4G",
    "numExecutors":4,
    "executorCores":3,
    "jars": ["wasb:///azure-documentdb-1.15.0.jar","wasb:///azure-cosmosdb-spark_2.1.0_2.11-1.0.0.jar"],
    "conf": {
        "spark.jars.packages": "com.microsoft.azure:azure-documentdb-rx:0.9.0-rc2",
        "spark.jars.excludes": "org.scala-lang:scala-reflect"
    }
}
```


## Working with the connector
You can build, download, or obtain the Azure Cosmos DB Spark connector as per below. 

### How to build the connector
Currently, this connector project uses `maven` so to build without dependencies, you can run:

```sh
mvn clean package
```

### Download the connector
You can also download the latest versions of the jar within the [releases](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases) folder.

The current version of the Spark connector which you can download directly is [azure-cosmosdb-spark_1.0.0_2.1.0_2.11](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-1.0.0):
* `azure-cosmosdb-spark` version: 1.0.0
* Apache Spark version: 2.2.0
* Scala version: 2.11

### Download from Maven
You can also download the JARs from maven:

| Spark | Scala | Latest version |
|---|---|---|
| 2.2.0 | 2.11 | [azure-cosmosdb-spark_1.0.0-2.2.0_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.2.0_2.11/1.0.0) |
| 2.2.0 | 2.10 | [azure-cosmosdb-spark_1.0.0-2.2.0_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.2.0_2.10/1.0.0) |
| 2.1.0 | 2.11 | [azure-cosmosdb-spark_1.0.0-2.1.0_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.1.0_2.11/1.0.0) |
| 2.1.0 | 2.10 | [azure-cosmosdb-spark_1.0.0-2.1.0_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.1.0_2.10/1.0.0) |
| 2.0.2 | 2.11 | [azure-cosmosdb-spark_0.0.3-2.0.2_2.11](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.0.2_2.11/0.0.3) |
| 2.0.2 | 2.10 | [azure-cosmosdb-spark_0.0.3-2.0.2_2.10](https://mvnrepository.com/artifact/com.microsoft.azure/azure-cosmosdb-spark_2.0.2_2.10/0.0.3) |



## Including the Azure Cosmos DB Spark JAR
Before you execute any code, you need to include the Azure Cosmos DB Spark JAR. If you are using the `spark-shell`, then you can include the JAR by using the `--jars` option.

```
spark-shell --master yarn --jars /$location/azure-cosmosdb-spark_2.1.0_2.11-1.0.0.jar,/$location/azure-documentdb-1.14.0.jar,/$location/azure-documentdb-rx-0.9.0-rc2.jar,/$location/json-20140107.jar,/$location/rxjava-1.3.0.jar,/$location/rxnetty-0.4.20.jar --num-executors 10 --executor-cores 2

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

The `jars` command enables you to include the two JARs that are needed for `azure-cosmosdb-spark` (itself and the Azure Cosmos DB SQL Java SDK) and exclude `scala-reflect` so that it does not interfere with the Livy calls (Jupyter notebook > Livy > Spark).

> Note, the above `spark magic` command makes the assumption that the JARs have been uploaded to the Azure HDInsight default storage account. For more information on how to do this, please refer to [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup) > [Step 2: Upload Spark Connector JAR to your HDI cluster's storage account](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup).



## Working with our samples

Included in this GitHub repository are a number of sample notebooks and scripts that you can utilize:
* **On-Time Flight Performance with Spark and Cosmos DB (Seattle)** [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/On-Time%20Flight%20Performance%20with%20Spark%20and%20Cosmos%20DB%20-%20Seattle.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/On-Time%20Flight%20Performance%20with%20Spark%20and%20Cosmos%20DB%20-%20Seattle.html): This notebook utilizing `azure-cosmosdb-spark` to connect Spark to Cosmos DB using HDInsight Jupyter notebook service to showcase Spark SQL, GraphFrames, and predicting flight delays using ML pipelines. 
* **[Connecting Spark with Cosmos DB Change feed](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Spark%2Band%2BCosmos%2BDB%2BChange%2BFeed.ipynb)**: A quick showcase on how to connect Spark to Cosmos DB Change Feed.
* **Twitter Source with Apache Spark and Azure Cosmos DB Change Feed**: [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Twitter%20with%20Spark%20and%20Azure%20Cosmos%20DB%20Change%20Feed.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Twitter%20with%20Spark%20and%20Azure%20Cosmos%20DB%20Change%20Feed.html)
* **Using Apache Spark to query Cosmos DB Graphs**: [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Using%20Apache%20Spark%20to%20query%20Cosmos%20DB%20Graphs.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/notebooks/Using%20Apache%20Spark%20to%20query%20Cosmos%20DB%20Graphs.html)
* **[Connecting Azure Databricks to Azure Cosmos DB](https://docs.databricks.com/spark/latest/data-sources/azure/cosmosdb-connector.html)** using `azure-cosmosdb-spark`.  Linked here is also an Azure Databricks version of the [On-Time Flight Performance notebook](https://github.com/dennyglee/databricks/tree/master/notebooks/Users/denny%40databricks.com/azure-databricks). 
* **[Lambda Architecture with Azure Cosmos DB and HDInsight (Apache Spark)](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/readme.md)**: Combining the Azure Cosmos DB, , and HDInsight not only allows you to accelerate real-time big data analytics, but also allows you to benefit from a Lambda Architecture while simplifying its operations.


## More Information

We have more information in the `azure-cosmosdb-spark` [wiki](https://github.com/Azure/azure-cosmosdb-spark/wiki) including:
* [Azure Cosmos DB Spark Connector User Guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-Cosmos-DB-Spark-Connector-User-Guide)
* [Aggregations Examples](https://github.com/Azure/azure-documentdb-spark/wiki/Aggregations-Examples)

Configuration and Setup
* [Spark Connector Configuration](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references)
* [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-documentdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup) (In progress)
* [Configuring Power BI Direct Query to Azure Cosmos DB via Apache Spark (HDI)](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuring-Power-BI-Direct-Query-to-Azure-Cosmos-DB-via-Apache-Spark-(HDI))

Troubleshooting
* [Using Cosmos DB Aggregates](https://github.com/Azure/azure-documentdb-spark/wiki/Troubleshooting:-Using-Cosmos-DB-Aggregates)
* [Known Issues](https://github.com/Azure/azure-cosmosdb-spark/wiki/Known-Issues)

Performance 
* [Performance Tips](https://github.com/Azure/azure-cosmosdb-spark/wiki/Performance-tips)
* [Query Test Runs](https://github.com/Azure/azure-documentdb-spark/wiki/Query-Test-Runs)
* [Writing Test Runs](https://github.com/Azure/azure-cosmosdb-spark/wiki/Writing-Test-Runs)

Change Feed
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)
* [Change Feed Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Change-Feed-demos)
* [Structured Stream Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)




