# azure-documentdb-spark
This project provides a client library that allows Azure DocumentDB to act as an input source or output sink for Spark jobs.

There will be two approaches for the **Spark-to-DocumentDB** connector:

* Using `pyDocumentDB`
* Create a Java-based Spark-DocumentDB connector based utilizing the [DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java) and based off of the [`FiloDB` Project](https://github.com/filodb/FiloDB)


## pyDocumentDB
The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `DocumentDB` via the following diagram flow.

![Spark to DocumentDB Data Flow via pyDocumentDB](documentation/images/Spark-DocumentDB_pyDocumentDB.png)

The data flow is as follows:

1. Connection is made from Spark master node to DocumentDB gateway node via `pyDocumentDB`.  Note, user only specifies Spark and DocumentDB connections, the fact that it connects to the respective master and gateway nodes is transparent to the user.
2. Query is made against DocuemntDB (via the gateway node) where the query subsequently runs the query against the collection's partitions in the data nodes.   The response for those queries is sent back to the gateway node and that resultset is returned to Spark master node.
3. Any subsequent queries (e.g. against a Spark DataFrame) is sent to the Spark worker nodes for processing.

The important call out is that communication between Spark and DocumentDB is limited to the Spark master node and DocumentDB gateway nodes.  The queries will go as fast as the transport layer is between these two nodes.


### Connecting Spark to DocumentDB via pyDocumentDB 
But in return for the simplicity of the communication transport, executing a query from Spark to DocumentDB using `pyDocumentDB` is relatively simple.

Below is a code snippet on how to use `pyDocumentDB` within a Spark context.

```
# Import Necessary Libraries
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime

# Configuring the connection policy (allowing for endpoint discovery)
connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["West US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]
#connectionPolicy.PreferredLocations = ["Southeast Asia", "Western Europe","Canada Central", "West US", "East US 2"]


# Set keys to connect to DocumentDB 
masterKey = 'le1n99i1w5l7uvokJs3RT5ZAH8dc3ql7lx2CG0h0kK4lVWPkQnwpRLyAN0nwS1z4Cyd1lJgvGUfMWR3v8vkXKA==' 
host = 'https://doctorwho.documents.azure.com:443/'
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)
```

As noted in the code snippet:

* The DocumentDB python SDK contains the all the necessary connection parameters including the preferred locations (i.e. choosing which read replica in what priority order).
*  Just impport the necessary libraires and configure your `masterKey` and `host` to create the DocumentDB *client* (`pydocumentdb.document_client`).


### Executing Spark Queries via pyDocumentDB
Below are a couple of examples using the above `DocumentDB` instance via the specified `read-only` keys.  This code snippet below connects to the `airports.codes` collection (in the DoctorWho account as specified earlier) running a query to extract the airport cities in Washington state. 

```
# Configure Database and Collections
databaseId = 'airports'
collectionId = 'codes'

# Configurations the DocumentDB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId


# Set query parameter
query = "SELECT c.City FROM c WHERE c.State='WA'"

# Query documents
query = client.QueryDocuments(collLink, query, options=None, partition_key=None)

# Query for partitioned collections
# query = client.QueryDocuments(collLink, query, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

# Push into list `elements`
elements = list(query)
```

Once the query has been executed via `query`, the result is a `query_iterable.QueryIterable` that is converted into a Python list. A Python list can be easily converted into a Spark DataFrame using the code below.

```
# Create `df` Spark DataFrame from `elements` Python list
df = spark.createDataFrame(elements)
```

### Performance 
Below are the results of connecting Spark to DocumentDB via `pyDocumentDB` with the following configuration:

* Single VM Spark cluster (one master, one worker) on Azure DS11 v2 VM (14GB RAM, 2 cores) running Ubuntu 16.04 LTS using Spark 2.1.
* DocumentDB single partition collection configured to 10,000 RUs
* `airport.codes` has 512 documents 
* `DepartureDelays.flights` has 1.05M documents (single collection)
* `DepartureDelays.flights (pColl)` has 1.39M documents (partitioned collection)

| Query | # of rows | Collection | Response Time (First) | Response Time (Second)| to DataFrame | 
| ----- | --------: | ---------- | --------------------- | --------------------- | ------------ |
| SELECT c.City FROM c WHERE c.State='WA' | 7 | airport.codes | 0:00:00.225645 | 0:00:00.006784 | 0:00:00.025026 |
| SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c | 100 | DepartureDelays.flights | 0:00:00.214985 | 0:00:00.009669 | 0:00:00.045043 |
| SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA' | 14,808 | DepartureDelays.flights | 0:00:01.498699 | 0:00:01.323917 | 0:00:00.740898 |
| SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c | 100 | DepartureDelays.flights (pColl) | 0:00:00.774820 | 0:00:00.508290 |  |
| SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA' | 23,078 | DepartureDelays.flights (pColl) | 0:00:05.146107 | 0:00:03.234670 |  |
| SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c | 1,048,575 | DepartureDelays.flights | 0:01:37.518344 | | | 
| SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c | 1,391,578 | DepartureDelays.flights (pColl) | 0:02:36.335267 | | | 




### Scenarios
Connecting Spark to DocumentDB using `pyDocumentDB` are typically for scenarios where:

* You want to use `python`
* You are returning a relatively small resultset from DocumentDB to Spark.  Note, the underlying dataset within DocumentDB can be quite large.  It is more that you are applying filters - i.e. running predicate filters - against your DocumentDB source.  


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




