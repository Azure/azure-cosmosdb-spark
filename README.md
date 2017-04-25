# azure-documentdb-spark
Preview connector for [Azure DocumentDB](http://documentdb.com) and [Apache Spark](http://spark.apache.org). 

Official instructions for using the connector are included in the DocumentDB documentation, in the [Accelerate real-time big-data analytics with the Spark to DocumentDB connector](https://docs.microsoft.com/azure/documentdb/documentdb-spark-connector) article.

This project provides a client library that allows Azure DocumentDB to act as an input source or output sink for Spark jobs.

> This connector is experimental and is provided as a public technical preview only

Officially supports Spark version: 2.0.2, Scala version: 2.11, Azure DocumentDB Java SDK: 1.10.0

There are currently two approaches to connect Apache Spark to Azure DocumentDB:

* Using `pyDocumentDB`
* Using `azure-documentdb-spark` - a Java-based Spark-DocumentDB connector based utilizing the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java)


See the [user guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-DocumentDB-Spark-Connector-User-Guide) for more information about the API.

## Requirements

* Apache Spark 2.0+
* Java Version >= 7.0
* If using Python
  * `pyDocumentDB` package
  * Python >= 2.7 or Python >= 3.3
* If using Scala
  * Azure DocumentDB Java SDK 1.10.0

For those using HDInsight, this has been tested on HDI 3.5


## How to connect Spark to DocumentDB using pyDocumentDB

The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `DocumentDB`. Here's a small code snippet that queries for airport codes from the DoctorWho Azure DocumentDB database; the results are in the `df` DataFrame.

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

# Set keys to connect to DocumentDB 
masterKey = 'le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==' 
host = 'https://doctorwho.documents.azure.com:443/'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

# Configure Database and Collections
databaseId = 'airports'
collectionId = 'codes'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId

# Set query parameter
querystr = "SELECT c.City FROM c WHERE c.State='WA'"

# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

# Query for partitioned collections
# query = client.QueryDocuments(collLink, query, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# Push into list `elements`
elements = list(query)

# Create `df` Spark DataFrame from `elements` Python list
df = spark.createDataFrame(elements)
```

## How to connect Spark to DocumentDB using azure-documentdb-spark

The `azure-documentdb-spark` connector connects Apache Spark to DocumentDB usign the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java).  Here's a small code snippet that queries for flight data from the DoctorWho Azure DocumentDB database; the results are in the `df` DataFrame.

```scala
// Import Necessary Libraries
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.documentdb.spark.schema._
import com.microsoft.azure.documentdb.spark._
import com.microsoft.azure.documentdb.spark.config.Config

// Configure connection to your collection
val readConfig2 = Config(Map("Endpoint" -> "https://doctorwho.documents.azure.com:443/",
"Masterkey" -> "le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==",
"Database" -> "DepartureDelays",
"preferredRegions" -> "Central US;East US2;",
"Collection" -> "flights_pcoll", 
"SamplingRatio" -> "1.0"))

// Create collection connection 
val coll = spark.sqlContext.read.DocumentDB(readConfig2)
coll.createOrReplaceTempView("c")

// Queries
var query = "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
val df = spark.sql(query)

// Run DF query (count)
df.count()
```

## How to build the connector
Currently, this connector project uses `maven` so to build without dependencies, you can run:

```sh
mvn clean package
```

You can also download the latest versions of the jar within the *releases* folder.




