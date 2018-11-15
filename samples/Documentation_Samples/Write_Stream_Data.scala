// Databricks notebook source
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming._

// Configure connection to Azure Cosmos DB Change Feed (Trades)
val ConfigMap = Map(
  // Account settings
  "Endpoint" -> "https://cdbtwitter.documents.azure.com:443/",
  "Masterkey" -> "AnfVszwZbztwIPLheHScaZXoFa7kbWSoYYf75wttJ5Z502xEbeY6RlwHshRuntMisX5bOUzwKa6JvEX00MJi2w==",
  "Database" -> "Twitterdb",
  "Collection" -> "Twittercoll",
  // Change feed settings
  "ReadChangeFeed" -> "true",
  "ChangeFeedStartFromTheBeginning" -> "true",
  "ChangeFeedCheckpointLocation" -> "dbfs:/cosmos-feed20",
  "ChangeFeedQueryName" -> "Structured Stream Read",
  "InferStreamSchema" -> "true"
)



// COMMAND ----------

// Start reading change feed of trades as a stream
var Streamdata = spark
  .readStream
  .format(classOf[CosmosDBSourceProvider].getName)
  .options(ConfigMap)
  .load()

val sinkConfigMap = Map(
  "Endpoint" -> "https://cdbtwitter.documents.azure.com:443/",
  "Masterkey" -> "AnfVszwZbztwIPLheHScaZXoFa7kbWSoYYf75wttJ5Z502xEbeY6RlwHshRuntMisX5bOUzwKa6JvEX00MJi2w==",
  "Database" -> "Twitterdb",
  "Collection" -> "sinkcollection",
  "checkpointLocation" -> "streamingcheckpointlocation3",
  "WritingBatchSize" -> "100",
  "Upsert" -> "true")



// COMMAND ----------

// Start the stream writer
val streamingQueryWriter = Streamdata.writeStream
  .format(classOf[CosmosDBSinkProvider].getName)
  .outputMode("append")
  .options(sinkConfigMap)
  .start()
