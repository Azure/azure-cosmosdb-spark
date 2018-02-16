//
// Streaming Query from Cosmos DB Change Feed
//
// Connect to Spark via spark-shell
// spark-shell --master yarn --jars /home/sshuser/jars/0.0.5/azure-cosmosdb-spark_2.1.0_2.11-0.0.5-SNAPSHOT.jar,/home/sshuser/jars/0.0.5/azure-documentdb-1.13.0.jar,/home/sshuser/jars/0.0.5/azure-documentdb-rx-0.9.0-rc2.jar,/home/sshuser/jars/0.0.5/json-20140107.jar,/home/sshuser/jars/0.0.5/rxjava-1.3.0.jar,/home/sshuser/jars/0.0.5/rxnetty-0.4.20.jar
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
"Endpoint" -> "https://rainier.documents.azure.com:443/",
"Masterkey" -> "lMdWMSAeooleaB5lRS1GhyoKly9lz8Q93kzo5GHBPyojroJilQD9PvK2qsQxh0n9uldn3ZULttsrRTGJL7u1lA==",
"Database" -> "seahawks",
"Collection" -> "tweets",
"ConnectionMode" -> "Gateway",
"ChangeFeedCheckpointLocation" -> "checkpointlocation",
"changefeedqueryname" -> "Streaming Query from Cosmos DB Change Feed Internal Count")

// Start reading change feed as a stream
var streamData = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(sourceConfigMap).load()

// Start streaming query to console sink
val query = streamData.withColumn("countcol", streamData.col("id").substr(0, 0)).groupBy("countcol").count().writeStream.outputMode("complete").format("console").start()
