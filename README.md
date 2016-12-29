# azure-documentdb-spark
This project provides a client library that allows Azure DocumentDB to act as an input source or output sink for Spark jobs.

There will be two approaches for the **Spark-to-DocumentDB** connector:

* Using `pyDocumentDB`
* Create a Java-based Spark-DocumentDB connector based utilizing the [DocumentDB Java SDK](https://github.com/Azure/azure-documentdb-java) and based off of the [`FiloDB` Project](https://github.com/filodb/FiloDB)


## pyDocumentDB
The current [`pyDocumentDB SDK`](https://github.com/Azure/azure-documentdb-python) allows us to connect `Spark` to `DocumentDB` via the following diagram flow.

<img src="https://github.com/Azure/azure-documentdb-spark/blob/master/documentation/images/Spark-DocumentDB_pyDocumentDB.png">

[![Spark to DocumentDB Data Flow via pyDocumentDB](documentation/images/Spark-DocumentDB_pyDocumentDB.png)]

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
* `airport.codes` has 512 documents while `DepartureDelays.flights` has 1.05M documents 

| Query | Collection | Response Time (First) | Response Time (Second)| to DataFrame | 
| ----- | ---------- | --------------------- | --------------------- | ------------ |
| SELECT c.City FROM c WHERE c.State='WA' (7 rows) | airport.codes | 0:00:00.225645 | 0:00:00.006784 | 0:00:00.025026 |
| SELECT TOP 100 c.date, c.delay, c.distance, c.origin, c.destination FROM c (100 rows)| DepartureDelays.flights | 0:00:00.214985 | 0:00:00.009669 | 0:00:00.045043 |
| SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA' (14,808 rows) | DepartureDelays.flights | 0:00:01.498699 | 0:00:01.323917 | 0:00:00.740898 |



### Scenarios
Connecting Spark to DocumentDB using `pyDocumentDB` are typically for scenarios where:

* You want to use `python`
* You are returning a relatively small resultset from DocumentDB to Spark.  Note, the underlying dataset within DocumentDB can be quite large.  It is more that you are applying filters - i.e. running predicate filters - against your DocumentDB source.  


