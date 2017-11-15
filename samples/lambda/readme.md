# Lambda Architecture with Azure Cosmos DB and HDInsight (Apache Spark)

**TODO:** Combining Azure Cosmos DB and HDInsight to create Lambda Architecture.  Need to include links and Cosmos DB and HDInsight.


## What is a Lambda Architecture?
A Lambda Architecture is a generic, scalable, and fault-tolerant data processing architecture to address batch and speed latency scenarios as described by [Nathan Marz](https://twitter.com/nathanmarz).
 
![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-intro.png)

<center>Source: http://lambda-architecture.net/</center>

The basic principles of a Lambda Architecture is described in the preceding diagram as per [https://lambda-architecture.net](http://lambda-architecture.net/).

 1. All **data** is pushed into *both* the *batch layer* and *speed layer*.
 2. The **batch layer** has a master dataset (immutable, append-only set of raw data) and pre-compute the batch views.
 3. The **serving layer** has batch views so data for fast queries. 
 4. The **speed layer** compensates for processing time (to serving layer) and deals with recent data only.
 5. All queries can be answered by merging results from batch views and real-time views or pinging them individually.

## Simplifying Operations via Azure Cosmos DB Change Feed
From an operations perspective, maintaining two streams of data while ensuring correct state for the data can be a complicated endeavor.  To simplify this, we can utilize [Azure Cosmos DB Change Feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed) to keep state for the *batch layer* while revealing the Azure Cosmos DB Change log via the *Change Feed API* for your *speed layer*.  

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-change-feed.png)


 1. All **data** is pushed *only* into Azure Cosmos DB thus you can avoid multi-casting issues.
 2. The **batch layer** has a master dataset (immutable, append-only set of raw data) and pre-compute the batch views.
 3. The **serving layer** will be discussed in the next section.
 4. The **speed layer** utilizes HDInsight (Apache Spark) that will read the Azure Cosmos DB Change Feed.  This allows you to persist your data as well as to query and process it concurrently.
 5. All queries can be answered by merging results from batch views and real-time views or pinging them individually.

 
### Code Example: Spark Structured Streaming to Azure Cosmos DB Change Feed
To run a quick prototype of the Azure Cosmos DB Change Feed as part of the **speed layer**, we can test it out using Twitter data as part of the [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark) example.  To jump start your Twitter output, please refer to the code sample in [Stream feed from Twitter to Cosmos DB](https://github.com/tknandu/TwitterCosmosDBFeed).  With the above example, you're loading Twitter data into Azure Cosmos DB and you can then setup your HDInsight (Apache Spark) cluster to connect to the change feed.  For more inforamtion on how to setup this configuration, please refer to [Apache Spark to Azure Cosmos DB Connector Setup](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup).  

Below is the code snippet on how to configure `spark-shell` to run a Structred Streaming job to connect to Azure Cosmos DB Change Feed to review the real-time Twitter data stream to perform a running interval count.

```
// Import Libraries
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import java.time._


// Configure connection to Azure Cosmos DB Change Feed
val sourceConfigMap = Map(
"Endpoint" -> "[COSMOSDB ENDPOINT]",
"Masterkey" -> "[MASTER KEY]",
"Database" -> "[DATABASE]",
"Collection" -> "[COLLECTION]",
"ConnectionMode" -> "Gateway",
"ChangeFeedCheckpointLocation" -> "checkpointlocation",
"changefeedqueryname" -> "Streaming Query from Cosmos DB Change Feed Interval Count")

// Start reading change feed as a stream
var streamData = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(sourceConfigMap).load()

// Start streaming query to console sink
val query = streamData.withColumn("countcol", streamData.col("id").substr(0, 0)).groupBy("countcol").count().writeStream.outputMode("complete").format("console").start()
```

> For complete code samples, please refer to [azure-cosmosdb-spark/lambda/samples](vhttps://github.com/Azure/azure-cosmosdb-spark/tree/master/samples/lambda) including:
> 
> * [Streaming Query from Cosmos DB Change Feed.scala](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Streaming%20Query%20from%20Cosmos%20DB%20Change%20Feed.scala)
> * [Streaming Tags Query from Cosmos DB Change Feed.scala](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Streaming%20Tags%20Query%20from%20Cosmos%20DB%20Change%20Feed%20.scala)
> 



The output of this of this is a `spark-shell` console continiously running a structured steraming job performing an interval count against the Twitter data from the Azure Cosmos DB Change Feed.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-speed-layer-twitter-count.png) 

### References
For more information on Azure Cosmos DB Change Feed, please refer to:

* [Working with the change feed support in Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed)
* [Introducing the Azure CosmosDB Change Feed Processor Library](https://azure.microsoft.com/en-us/blog/introducing-the-azure-cosmosdb-change-feed-processor-library/)
* [Stream Processing Changes: Azure CosmosDB change feed + Apache Spark](https://azure.microsoft.com/en-us/blog/stream-processing-changes-azure-cosmosdb-change-feed-apache-spark/)


## Building up Batch and Serving Layers
Since the data is loaded into Azure Cosmos DB (where the change feed is being used for the speed layer), this is where the **master dataset** (an immutable, append-only set of raw data) resides. From this point onwards, we can use HDInsight (Apache Spark) to perform our pre-compute from **batch layer** to **serving layer**.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-batch-serve.png)


 1. All **data** pushed into only Azure Cosmos DB (avoid multi-cast issues)
 2. The **batch layer** has a master dataset (immutable, append-only set of raw data) stored in Azure Cosmos DB. Using HDI Spark, you can pre-compute your aggregations to be stored in your computed batch views.
 3. The **serving layer** is an Azure Cosmos DB database with collections for master dataset and computed batch view.
 4. The **speed layer** will be discussed next slide.
 5. All queries can be answered by merging results from batch views and real-time views or pinging them individually.


## Lambda Architecture: Re-architected
This samples folder the Lambda Architecture: Re-architected with Apache Spark and Azure Cosmos DB per the diagram below.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-re-architected.png)







## References

The samples included are

 1. We will use the [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed) as our mechanism to push **new data** into Cosmos DB.
 2. The **batch layer** is comprised of the *master dataset* (an immutable, append-only set of raw data) and ethe ability to pre-compute batch views of the data that will be pushed into the **serving layer**
    * The [Lambda Architecture Re-architected - Batch Layer]() notebook [ipynb]() | [html]() queries the *master dataset* set of batch views.
 3. The **serving layer** is comprised of pre-computed data resulting in batch views (e.g. aggregations, specific slicers, etc.) for fast queries.
    * The [Lambda Architecture Re-architected - Batch to Serving Layer]() notebook [ipynb]() | [html]() pushes the *batch* data to the *serving layer*; i.e. Spark will query a batch collection of tweets, process it, and store it into another collection (i.e. *computed batch*).
 4. The **speed layer** is comprised of Spark utilizing Cosmos DB change feed to read and act on immediately.  The data can also be saved to *computed RT* so that other systems can query the processed real-time data as opposed to running a real-time query themselves.
    * The [Streaming Query from Cosmos DB Change Feed]() scala script is to be used by spark-shell to execute a streaming query from Cosmos DB Change Feed to compute an interval count.
    * The [Streaming Tags Query from Cosmos DB Change Feed ]() scala script is to be used by spark-shell to execute a streaming query from Cosmos DB Change Feed to compute an interval count by tags.
  

## Requirements
* Azure Cosmos DB Collection
* HDI (Apache Spark 2.1) cluster
* Spark Connector: We are currently using the [0.0.5-SNAPSHOT](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-0.0.5-SNAPSHOT) version of the connector.

## References
* [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed)
* [Spark to Cosmos DB Connector Setup](https://github.com/Azure/azure-cosmosdb-spark/wiki/Spark-to-Cosmos-DB-Connector-Setup)
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)
* [Structured Stream demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)

