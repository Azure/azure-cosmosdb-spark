// Databricks notebook source
//
// Streaming Query from Cosmos DB Change Feed
//
// This script does the following:
// - creates a structured stream from a Twitter feed CosmosDB collection (on top of change feed)
// - get the count of tweets
//

import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._
import java.time._

val sourceConfigMap = Map(
"Endpoint" -> "https://cdbtwitter.documents.azure.com:443/",
"Masterkey" -> "AnfVszwZbztwIPLheHScaZXoFa7kbWSoYYf75wttJ5Z502xEbeY6RlwHshRuntMisX5bOUzwKa6JvEX00MJi2w==",
"Database" -> "Twitterdb",
"Collection" -> "Twittercoll",
"ConnectionMode" -> "Gateway",
"ChangeFeedCheckpointLocation" -> "/tmp",
"changefeedqueryname" -> "Streaming Query from Cosmos DB Change Feed Internal Count")



// COMMAND ----------


// Start reading change feed as a stream
var streamData =
spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(sourceConfigMap).
load()


// COMMAND ----------

//**RUN THE ABOVE FIRST AND KEEP BELOW IN SEPARATE CELL
// Start streaming query to console sink
val query = streamData.withColumn("countcol", streamData.col("id").substr(0,
0)).groupBy("countcol").count().writeStream.outputMode("complete").format("console").start
()
