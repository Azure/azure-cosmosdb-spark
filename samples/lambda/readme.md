# Lambda Architecture with Azure Cosmos DB and HDInsight (Apache Spark)

Combining the [Azure Cosmos DB](https://azure.microsoft.com/blog/azure-cosmos-db-microsofts-globally-distributed-multi-model-database-service/), the industry's first globally-distributed, multi-model database service, and HDInsight not only allows you to [accelerate real-time big data analytics](https://docs.microsoft.com/en-us/azure/cosmos-db/spark-connector), but also allows you to benefit from a **Lambda Architecture** while simplifying its operations.  

[![](https://raw.githubusercontent.com/dennyglee/notebooks/master/images/135_Connect.png)](https://channel9.msdn.com/Events/Connect/2017/T135/player)

For a quick overview of the various notebooks and components for our Lambda Architecture samples, please refer to the Channel 9 video [Real-time Analytics with Azure Cosmos DB and Apache Spark](https://channel9.msdn.com/Events/Connect/2017/T135).  


>  ### Requirements
> * Azure Cosmos DB Collection(s)
> * HDI (Apache Spark 2.1) cluster
> * Spark Connector: We are currently using the [1.0](https://github.com/Azure/azure-cosmosdb-spark/tree/master/releases/azure-cosmosdb-spark_2.1.0_2.11-1.0.0) version of the connector.



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

## Speed Layer: Simplifying Operations via Azure Cosmos DB Change Feed
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

### Code Example: Pre-computing Batch Views
To showcase how to execute pre-calculated views against your **master dataset** from Apache Spark to Azure Cosmos DB, below are code snippets from the notebooks [Lambda Architecture Re-architected - Batch Layer](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20Layer.ipynb) and [Lambda Architecture Re-architected - Batch to Serving Layer](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20to%20Serving%20Layer.ipynb). In this scenario, we will be using Twitter data stored in Cosmos DB.

* Let's start by creating the configuration connection to our Twitter data within Azure Cosmos DB using the PySpark code below.

```
# Configuration to connect to Azure Cosmos DB
tweetsConfig = {
  "Endpoint" : "[Endpoint URL]",
  "Masterkey" : "[Master Key]",
  "Database" : "[Database]",
  "Collection" : "[Collection]", 
  "preferredRegions" : "[Preferred Regions]",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "200000",
  "query_custom" : "[Cosmos DB SQL Query]"
}

# Create DataFrame
tweets = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**tweetsConfig).load()

# Create Temp View (to run Spark SQL statements)
tweets.createOrReplaceTempView("tweets")
```

*  Next, let's run the following Spark SQL statement to determine the top 10 hashtags of our set of tweets.  Note, for this Spark SQL query, we're running this in a Jupyter notebook without the output bar chart directly below this code snippet.

```
%%sql
select hashtags.text, count(distinct id) as tweets
from (
  select 
    explode(hashtags) as hashtags,
    id
  from tweets
) a
group by hashtags.text
order by tweets desc
limit 10
```

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-batch-hashtags-bar-chart.png)

* Now that you have your query, let's save it back to a collection by using the Spark Connector to save the output data into a different collection.  In this example, we will use Scala to showcase the connection.  Similar to the previous example, creating the configuration connection to save our Apache Spark DataFrame to a different Azure Cosmos DB collection.

```
val writeConfigMap = Map(
	"Endpoint" -> "[Endpoint URL]",
	"Masterkey" -> "[Master Key]",
	"Database" -> "[Database]",
	"Collection" -> "[New Collection]", 
	"preferredRegions" -> "[Preferred Regions]",
	"SamplingRatio" -> "1.0",
	"schema_samplesize" -> "200000"
)

// Configuration to write
val writeConfig = Config(writeConfigMap)

```

* After specifying the `SaveMode` (i.e. whether to Overwrite or Append documents), we create `tweets_bytags` DataFrame similar to the Spark SQL query in the previous example.  With the `tweets_bytags` DataFrame created, you can save it using the `write` method using the previously specified `writeConfig`.

```
// Import SaveMode so you can Overwrite, Append, ErrorIfExists, Ignore
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

// Create new DataFrame of tweets tags
val tweets_bytags = spark.sql("select hashtags.text as hashtags, count(distinct id) as tweets from ( select explode(hashtags) as hashtags, id from tweets ) a group by hashtags.text order by tweets desc")

// Save to Cosmos DB (using Append in this case)
tweets_bytags.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
```

This last statement now has saved your Spark DataFrame into a new Azure Cosmos DB collection; from a lambda architecture perspective, this is your **batch view** within the **serving layer**.
 
> For complete code samples, please refer to [azure-cosmosdb-spark/lambda/samples](vhttps://github.com/Azure/azure-cosmosdb-spark/tree/master/samples/lambda) including:
> 
> * Lambda Architecture Re-architected - Batch Layer [HTML](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20Layer.html) | [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20Layer.ipynb)
> * Lambda Architecture Re-architected - Batch to Serving Layer [HTML](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20to%20Serving%20Layer.html) | [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20to%20Serving%20Layer.ipynb)
> 


## Persisting the Speed Layer
As previously noted, using Azure Cosmos DB Change Feed allows us to simplfy the operations between the batch and speed layers.  In this architecture, we are using Apache Spark (via HD Insight) to perform our *structured streaming* queries against the data.   But you may also want to temporarily persist the results of your structured streaming queries so other systems can access this data.

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-speed.png)

To do this, we can create a separate Cosmos DB collection to save the results of your structured streaming queries.  This allows you to have other systems access this information not just Apache Spark. As well with the Cosmos DB Time-to-Live (TTL) feature, you can configure your documents to be automatically deleted after a set duration.  For more information on the Azure Cosmos DB TTL feature, please refer to [Expire data in Azure Cosmos DB collections automatically with time to live](https://docs.microsoft.com/azure/cosmos-db/time-to-live)

```
// Import Libraries
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import java.time._


// Configure connection to Azure Cosmos DB Change Feed
val sourceCollectionName = "[SOURCE COLLECTION NAME]"
val sinkCollectionName = "[SINK COLLECTION NAME]"

val configMap = Map(
"Endpoint" -> "[COSMOSDB ENDPOINT]",
"Masterkey" -> "[COSMOSDB MASTER KEY]",
"Database" -> "[DATABASE NAME]",
"Collection" -> sourceCollectionName,
"ChangeFeedCheckpointLocation" -> "changefeedcheckpointlocation")

val sourceConfigMap = configMap.+(("changefeedqueryname", "Structured Stream replication streaming test"))

// Start to read the stream
var streamData = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(sourceConfigMap).load()
val sinkConfigMap = configMap.-("collection").+(("collection", sinkCollectionName))

// Start the stream writer to new collection
val streamingQueryWriter = streamData.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("append").options(sinkConfigMap).option("checkpointLocation", "streamingcheckpointlocation")
var streamingQuery = streamingQueryWriter.start()

```


## Lambda Architecture: Re-architected
As noted in the previous sections, we can simplify our Lambda Architecture by using Azure Cosmos DB, the Cosmos DB Change Feed to avoid the need to multi-cast your data between the batch and speed layers, Apache Spark on HDInsight, and the Spark Connector for Azure Cosmos DB. 

![](https://raw.githubusercontent.com/Azure/azure-cosmosdb-spark/master/docs/images/scenarios/lambda-architecture-re-architected.png)

With this design, we need only two two managed services that together will address the batch, serving, and speed layers of our Lambda Architecture simplifying not only the operations but also the data flow. 
 1. All data pushed into Cosmos DB layer for processing
 2. The batch layer has a master dataset (immutable, append-only set of raw data) and pre-compute the batch views
 3. The serving layer has batch views so data for fast queries.
 4. The speed layer compensates for processing time (to serving layer) and deals with recent data only.
 5. All queries can be answered by merging results from batch views and real-time views.


### References

The samples included are

 1. We will use the [Stream feed from Twitter to CosmosDB](https://github.com/tknandu/TwitterCosmosDBFeed) as our mechanism to push **new data** into Cosmos DB.
 2. The **batch layer** is comprised of the *master dataset* (an immutable, append-only set of raw data) and the ability to pre-compute batch views of the data that will be pushed into the **serving layer**
    * The **Lambda Architecture Re-architected - Batch Layer** notebook [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20Layer.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20Layer.html) queries the *master dataset* set of batch views.
 3. The **serving layer** is comprised of pre-computed data resulting in batch views (e.g. aggregations, specific slicers, etc.) for fast queries.
    * The **Lambda Architecture Re-architected - Batch to Serving Layer** notebook [ipynb](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20to%20Serving%20Layer.ipynb) | [html](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Lambda%20Architecture%20Re-architected%20-%20Batch%20to%20Serving%20Layer.html) pushes the *batch* data to the *serving layer*; i.e. Spark will query a batch collection of tweets, process it, and store it into another collection (i.e. *computed batch*).
 4. The **speed layer** is comprised of Spark utilizing Cosmos DB change feed to read and act on immediately.  The data can also be saved to *computed RT* so that other systems can query the processed real-time data as opposed to running a real-time query themselves.
    * The [Streaming Query from Cosmos DB Change Feed](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Streaming%20Query%20from%20Cosmos%20DB%20Change%20Feed.scala) scala script is to be used by spark-shell to execute a streaming query from Cosmos DB Change Feed to compute an interval count.
    * The [Streaming Tags Query from Cosmos DB Change Feed](https://github.com/Azure/azure-cosmosdb-spark/blob/master/samples/lambda/Streaming%20Tags%20Query%20from%20Cosmos%20DB%20Change%20Feed%20.scala) scala script is to be used by spark-shell to execute a streaming query from Cosmos DB Change Feed to compute an interval count by tags.
  
## Next Steps
If you haven't already, download the Spark to Azure Cosmos DB connector from the [azure-cosmosdb-spark](https://github.com/Azure/azure-cosmosdb-spark) GitHub repository and explore the additional resources in the repo:
* [Lambda Architecture](https://github.com/Azure/azure-cosmosdb-spark/tree/master/samples/lambda)
* [Distributed Aggregations Examples](https://github.com/Azure/azure-documentdb-spark/wiki/Aggregations-Examples)
* [Sample Scripts and Notebooks](https://github.com/Azure/azure-cosmosdb-spark/tree/master/samples)
* [Structured Streaming Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Structured-Stream-demos)
* [Change Feed Demos](https://github.com/Azure/azure-cosmosdb-spark/wiki/Change-Feed-demos)
* [Stream Processing Changes using Azure Cosmos DB Change Feed and Apache Spark](https://github.com/Azure/azure-cosmosdb-spark/wiki/Stream-Processing-Changes-using-Azure-Cosmos-DB-Change-Feed-and-Apache-Spark)

You might also want to review the [Apache Spark SQL, DataFrames, and Datasets Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html) and the [Apache Spark on Azure HDInsight](https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-jupyter-spark-sql) article.

