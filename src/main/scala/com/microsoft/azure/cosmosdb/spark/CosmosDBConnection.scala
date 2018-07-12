/**
  * The MIT License (MIT)
  * Copyright (c) 2016 Microsoft Corporation
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package com.microsoft.azure.cosmosdb.spark
import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.internal._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import java.lang.management.ManagementFactory

object CosmosDBConnection {
  // For verification purpose
  var lastConnectionPolicy: ConnectionPolicy = _
  var lastConsistencyLevel: Option[ConsistencyLevel] = _
}

private[spark] case class CosmosDBConnection(config: Config) extends LoggingTrait with Serializable {

  case class ClientConfiguration(host: String,
                                 key: String,
                                 connectionPolicy: ConnectionPolicy,
                                 consistencyLevel: ConsistencyLevel)

  private val databaseName = config.get[String](CosmosDBConfig.Database).get
  private val collectionName = config.get[String](CosmosDBConfig.Collection).get
  val collectionLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName/${Paths.COLLECTIONS_PATH_SEGMENT}/$collectionName"
  private val connectionMode = ConnectionMode.valueOf(config.get[String](CosmosDBConfig.ConnectionMode)
    .getOrElse(CosmosDBConfig.DefaultConnectionMode))
  private var collection: DocumentCollection = _
  @transient private var client: DocumentClient = _

  @transient private var bulkImporter: DocumentBulkExecutor = _

  private lazy val documentClient: DocumentClient = {
    if (client == null) {
      client = accquireClient(connectionMode)
      CosmosDBConnection.lastConnectionPolicy = client.getConnectionPolicy
    }
    client
  }

  def getDocumentBulkImporter(collectionThroughput: Int, partitionKeyDefinition: Option[String]): DocumentBulkExecutor = {
    if (bulkImporter == null) {
      val initializationRetryOptions = new RetryOptions()
      initializationRetryOptions.setMaxRetryAttemptsOnThrottledRequests(1000)
      initializationRetryOptions.setMaxRetryWaitTimeInSeconds(1000)

      if (partitionKeyDefinition.isDefined) {
        val pkDefinition = new PartitionKeyDefinition()
        val paths: ListBuffer[String] = new ListBuffer[String]()
        paths.add(partitionKeyDefinition.get)
        pkDefinition.setPaths(paths)

        bulkImporter = DocumentBulkExecutor.builder.from(documentClient,
          databaseName,
          collectionName,
          pkDefinition,
          collectionThroughput
        ).withInitializationRetryOptions(initializationRetryOptions).build()
      }
      else {
        bulkImporter = DocumentBulkExecutor.builder.from(documentClient,
          databaseName,
          collectionName,
          getCollection.getPartitionKey,
          collectionThroughput
        ).withInitializationRetryOptions(initializationRetryOptions).build()
      }
    }

    bulkImporter
  }

  def setDefaultClientRetryPolicy: Unit = {
    if (documentClient != null) {
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(9);
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(30);
    }
  }

  def setZeroClientRetryPolicy: Unit = {
    if (documentClient != null) {
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
    }
  }

  def getCollection: DocumentCollection = {
    if (collection == null) {
      collection = documentClient.readCollection(collectionLink, null).getResource
    }
    collection
  }

  def getAllPartitions: Array[PartitionKeyRange] = {
    var ranges = documentClient.readPartitionKeyRanges(collectionLink, null.asInstanceOf[FeedOptions])
    ranges.getQueryIterator.toArray
  }

  def getAllPartitions(query: String): Array[PartitionKeyRange] = {
    var ranges: java.util.Collection[PartitionKeyRange] =
      documentClient.readPartitionKeyRanges(collectionLink, query)
    ranges.toArray[PartitionKeyRange](new Array[PartitionKeyRange](ranges.size()))
  }

  def getCollectionThroughput: Int = {
    val offers = documentClient.queryOffers(s"SELECT * FROM c where c.offerResourceId = '${getCollection.getResourceId}'", null).getQueryIterable.toList
    if (offers.isEmpty) {
      throw new IllegalStateException("Cannot find Collection's corresponding offer")
    }
    val offer = offers.get(0)
    val collectionThroughput = if (offer.getString("offerVersion") == "V1")
      CosmosDBConfig.SinglePartitionCollectionOfferThroughput
    else
      offer.getContent.getInt("offerThroughput")
    collectionThroughput
  }

  def queryDocuments (queryString : String,
                      feedOpts : FeedOptions) : Iterator [Document] = {
    Console.println("SparkConnector feed options for pk range " + feedOpts.getPartitionKeyRangeIdInternal + " : " + feedOpts.toString)
    try {
      val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
      feedResponse.getQueryIterable.iterator()
    } catch {
      case dce: DocumentClientException => {
        Console.println("SparkConnector failed for partition key range " + feedOpts.getPartitionKeyRangeIdInternal)
        throw dce
      }
    }

  }

  def queryDocuments (collectionLink: String, queryString : String,
                      feedOpts : FeedOptions) : Iterator [Document] = {
    val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    feedResponse.getQueryIterable.iterator()
  }

  def readDocuments(feedOptions: FeedOptions): Iterator[Document] = {
    documentClient.readDocuments(collectionLink, feedOptions).getQueryIterable.iterator()
  }

  def readChangeFeed(changeFeedOptions: ChangeFeedOptions, isStreaming: Boolean, shouldInferStreamSchema: Boolean): Tuple2[Iterator[Document], String] = {
    val feedResponse = documentClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)
    if (isStreaming) {
      // In streaming scenario, the change feed need to be materialized in order to get the information of the continuation token
      val cfDocuments: ListBuffer[Document] = new ListBuffer[Document]
      while (feedResponse.getQueryIterator.hasNext) {
        val feedItems = feedResponse.getQueryIterable.fetchNextBlock()
        if (shouldInferStreamSchema)
        {
          cfDocuments.addAll(feedItems)
        } else {
          for (feedItem <- feedItems) {
            val streamDocument: Document = new Document()
            streamDocument.set("body", feedItem.toJson)
            streamDocument.set("id", feedItem.get("id"))
            streamDocument.set("_rid", feedItem.get("_rid"))
            streamDocument.set("_self", feedItem.get("_self"))
            streamDocument.set("_etag", feedItem.get("_etag"))
            streamDocument.set("_attachments", feedItem.get("_attachments"))
            streamDocument.set("_ts", feedItem.get("_ts"))
            cfDocuments.add(streamDocument)
          }
        }
        logDebug(s"Receving change feed items ${if (feedItems.nonEmpty) feedItems(0)}")
      }
      Tuple2.apply(cfDocuments.iterator(), feedResponse.getResponseContinuation)
    } else {
      Tuple2.apply(feedResponse.getQueryIterator, feedResponse.getResponseContinuation)
    }
  }

  def upsertDocument(collectionLink: String,
                     document: Document,
                     requestOptions: RequestOptions): Unit = {
    logTrace(s"Upserting document $document")
    documentClient.upsertDocument(collectionLink, document, requestOptions, false)
  }

  def isDocumentCollectionEmpty: Boolean = {
    logDebug(s"Reading collection $collectionLink")
    var requestOptions = new RequestOptions
    requestOptions.setPopulateQuotaInfo(true)
    val response = documentClient.readCollection(collectionLink, requestOptions)
    if (collection == null) {
      collection = response.getResource
    }
    response.getDocumentCountUsage == 0
  }

  private def accquireClient(connectionMode: ConnectionMode): DocumentClient = {
    val clientConfiguration = getClientConfiguration(config)

    var documentClient = new DocumentClient(
      clientConfiguration.host,
      clientConfiguration.key,
      clientConfiguration.connectionPolicy,
      clientConfiguration.consistencyLevel)
    CosmosDBConnection.lastConsistencyLevel = Some(clientConfiguration.consistencyLevel)
    documentClient
  }

  private def getClientConfiguration(config: Config): ClientConfiguration = {
    // Generate connection policy
    val connectionPolicy = new ConnectionPolicy()

    connectionPolicy.setConnectionMode(connectionMode)
    // Merging the Spark connector version with Spark executor process id for user agent
    connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix + " " + ManagementFactory.getRuntimeMXBean().getName())

    config.get[String](CosmosDBConfig.ConnectionMaxPoolSize) match {
      case Some(maxPoolSizeStr) => connectionPolicy.setMaxPoolSize(maxPoolSizeStr.toInt)
      case None => // skip
    }
    config.get[String](CosmosDBConfig.ConnectionIdleTimeout) match {
      case Some(connectionIdleTimeoutStr) => connectionPolicy.setIdleConnectionTimeout(connectionIdleTimeoutStr.toInt)
      case None => // skip
    }

    val maxRetryAttemptsOnThrottled = config.getOrElse[String](CosmosDBConfig.QueryMaxRetryOnThrottled, CosmosDBConfig.DefaultQueryMaxRetryOnThrottled.toString)
    connectionPolicy.getRetryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttemptsOnThrottled.toInt)

    val maxRetryWaitTimeSecs = config.getOrElse[String](CosmosDBConfig.QueryMaxRetryWaitTimeSecs, CosmosDBConfig.DefaultQueryMaxRetryWaitTimeSecs.toString)
    connectionPolicy.getRetryOptions.setMaxRetryWaitTimeInSeconds(maxRetryWaitTimeSecs.toInt)

    val preferredRegionsList = config.get[String](CosmosDBConfig.PreferredRegionsList)
    if (preferredRegionsList.isDefined) {
      logTrace(s"CosmosDBConnection::Input preferred region list: ${preferredRegionsList.get}")
      val preferredLocations = preferredRegionsList.get.split(";").toSeq.map(_.trim)
      connectionPolicy.setPreferredLocations(preferredLocations)
    }

    // Get consistency level
    val consistencyLevel = ConsistencyLevel.valueOf(config.get[String](CosmosDBConfig.ConsistencyLevel)
      .getOrElse(CosmosDBConfig.DefaultConsistencyLevel))

    ClientConfiguration(
      config.get[String](CosmosDBConfig.Endpoint).get,
      config.get[String](CosmosDBConfig.Masterkey).get,
      connectionPolicy,
      consistencyLevel
    )
  }

}