# azure-documentdb-spark
Experimental connector for [Azure DocumentDB](http://documentdb.com) and [Apache Spark](http://spark.apache.org).

This project provides a client library that allows Azure DocumentDB to act as an input source or output sink for Spark jobs.

> This connector is experimental and is provided as a public technical preview only

Officially supports Spark version: 2.0.2, Scala version: 2.11, Azure DocumentDB Java SDK: 1.9.6

There are currently two approaches to connect Apache Spark to Azure DocumentDB:

* Using `pyDocumentDB`
* Using `azure-documentdb-spark` - a Java-based Spark-DocumentDB connector based utilizing the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java)


See the [user guide](https://github.com/Azure/azure-documentdb-spark/wiki/Azure-DocumentDB-Spark-Connector-User-Guide) for more information about the API.

# Requirements

* Apache Spark 2.0+
* Java Version >= 7.0
* If using Python
 * `pyDocumentDB` package
 * Python >= 2.7 or Python >= 3.3
* If using Scala
 * Azure DocumentDB Java SDK 1.9.6

For those using HDInsight, this has been tested on HDI 3.5


# How to connect Spark to DocumentDB using pyDocumentDB

The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `DocumentDB`. Here's a small code snippet that queries for airport codes from the DoctorWho Azure DocumentDB database; the results are in the `df` DataFrame.

```
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



## Spark to DocumentDB Connector
**IMPORTANT: Design Stage**

The Spark to DocumentDB connector that will utilize the [Azure DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java) will utilize the following flow:

![Spark to DocumentDB via Azure DocumentDB Java SDK](documentation/images/Spark-DocumentDB_JavaFiloDB.png)

The data flow is as follows:

1. Connection is made from Spark master node to DocumentDB gateway node to obtain the partition map.  Note, user only specifies Spark and DocumentDB connections, the fact that it connects to the respective master and gateway nodes is transparent to the user.
2. This information is provided back to the Spark master node.  At this point, we should be able to parse the query to determine which partitions (and their locations) within DocumentDB we need to access.
3. This information is transmitted to the Spark worker nodes ...
4. Thus allowing the Spark worker nodes to connect directly to the DocumentDB partitions directly to extract the data that is needed and bring the data back to the Spark partitions within the Spark worker nodes.


To perform this flow, we will need to develop the following:

1. Ensure the DocumentDB Java SDK can parse the SQL queries and pass the partition map information *in Linux*.
2. Utilize Evan Chan's [`FiloDB project`](https://github.com/filodb/FiloDB).  This project utilizes Spark DataFrames, Cassandra, and Parquet to provide updateable, columnar query performance that is distributed and versioned.  

The key component of the `FiloDB` project for partitioning and data locality (to perform steps 3 and 4 above) is within `FiloRelation.scala`.  Specficially,

```
FiloRelation.scala:
    val splitsWithLocations = splits.map { s => (s, s.hostnames.toSeq) }
    // NOTE: It's critical that the closure inside mapPartitions only references
    // vars from buildScan() method, and not the FiloRelation class.  Otherwise
    // the entire FiloRelation class would get serialized.
    // Also, each partition should only need one param.
    sqlContext.sparkContext.makeRDD(splitsWithLocations)
      .mapPartitions { splitIter =>
        perPartitionRowScanner(config, readOnlyProjStr, version, f(splitIter.next))
      }
```

This `splitsWithLocations` basically create an RDD with list of hostnames attached to each item which is a “split” in Cassandra of a token range or series of token ranges.  Therefore your reads will be local on each partition.




