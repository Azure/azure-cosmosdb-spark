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

import java.lang.management.ManagementFactory
import java.util.Collection

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.util.JacksonWrapper
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.internal._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions


case class ClientConfiguration(host: String,
                               key: String,
                               connectionPolicy: ConnectionPolicy,
                               consistencyLevel: ConsistencyLevel)
object CosmosDBConnection extends CosmosDBLoggingTrait {
  // For verification purpose
  var lastConnectionPolicy: ConnectionPolicy = _
  var lastConsistencyLevel: Option[ConsistencyLevel] = _
  var clients: scala.collection.mutable.Map[String, DocumentClient] = scala.collection.mutable.Map[String,DocumentClient]()

  def getClient(connectionMode: ConnectionMode, clientConfiguration: ClientConfiguration): DocumentClient = synchronized {
      val cacheKey = clientConfiguration.host + "-" + clientConfiguration.key
      if (!clients.contains(cacheKey)) {
          logInfo(s"Initializing new client for host ${clientConfiguration.host}")
          clients(cacheKey) = new DocumentClient(
          clientConfiguration.host,
          clientConfiguration.key,
          clientConfiguration.connectionPolicy,
          clientConfiguration.consistencyLevel)
          CosmosDBConnection.lastConsistencyLevel = Some(clientConfiguration.consistencyLevel)
      }

      clients.get(cacheKey).get
   }

   def reinitializeClient(connectionMode: ConnectionMode, clientConfiguration: ClientConfiguration): DocumentClient = synchronized {
     val cacheKey = clientConfiguration.host + "-" + clientConfiguration.key
     if(clients.get(cacheKey).nonEmpty) {
       logInfo(s"Reinitializing client for host ${clientConfiguration.host}")
       val client = clients.get(cacheKey).get
       client.close()
       CosmosDBConnection.clients.remove(cacheKey)
     }

     getClient(connectionMode, clientConfiguration)
   }
 }

private[spark] case class CosmosDBConnection(config: Config) extends CosmosDBLoggingTrait with Serializable {

  private val databaseName = config.get[String](CosmosDBConfig.Database).get
  private val databaseLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName"
  private val collectionName = config.get[String](CosmosDBConfig.Collection).get
  val collectionLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName/${Paths.COLLECTIONS_PATH_SEGMENT}/$collectionName"
  private val connectionMode = ConnectionMode.valueOf(config.get[String](CosmosDBConfig.ConnectionMode)
    .getOrElse(CosmosDBConfig.DefaultConnectionMode))
  private var collection: DocumentCollection = _
  private var database: Database = _

  @transient private var bulkImporter: DocumentBulkExecutor = _

  private var documentClient: DocumentClient = CosmosDBConnection.getClient(connectionMode, getClientConfiguration(config))


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
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(1000);
      documentClient.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(1000);
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

  def getDatabase: Database = {
    if (database == null) {
      database = documentClient.readDatabase(databaseLink, null).getResource
    }
    database
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
    var offers = documentClient.queryOffers(s"SELECT * FROM c where c.offerResourceId = '${getCollection.getResourceId}'", null).getQueryIterable.toList
    if (offers.isEmpty) {
      offers = documentClient.queryOffers(s"SELECT * FROM c where c.offerResourceId = '${getDatabase.getResourceId}'", null).getQueryIterable.toList
      // database throughput
      if (offers.isEmpty) {
          throw new IllegalStateException("Cannot find the collection corresponding offer.")
      }
    }

    val offer = offers.get(0)
    val collectionThroughput = if (offer.getString("offerVersion") == "V1")
      CosmosDBConfig.SinglePartitionCollectionOfferThroughput
    else
      offer.getContent.getInt("offerThroughput")
    collectionThroughput
  }

  def reinitializeClient () = {
    documentClient = CosmosDBConnection.reinitializeClient(connectionMode, getClientConfiguration(config))
  }

  def queryDocuments (queryString : String,
        feedOpts : FeedOptions) : Iterator [Document] = {
    val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    feedResponse.getQueryIterable.iterator()
  }

  def queryDocuments (collectionLink: String, queryString : String,
                      feedOpts : FeedOptions) : Iterator [Document] = {
    val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    feedResponse.getQueryIterable.iterator()
  }

  def readSchema(schemaType : String) = {
    val partitionKeyDefinition = getCollection.getPartitionKey
    val partitionKeyPath = partitionKeyDefinition.getPaths
    val partitionKeyProperty = partitionKeyPath.iterator.next.replaceFirst("^/", "")

    val feedOptions = new FeedOptions()
    feedOptions.setEnableCrossPartitionQuery(true)
    var schemaDocument : ItemSchema = null
    val response = documentClient.queryDocuments(collectionLink, new SqlQuerySpec("Select * from c where c.schemaType = '" + schemaType + "' and c." + partitionKeyProperty + " = '__schema__" + schemaType + "'"), feedOptions);
    val schemaResponse = response.getQueryIterable.fetchNextBlock()
    if(schemaResponse != null && !schemaResponse.isEmpty) {
      schemaDocument = JacksonWrapper.deserialize[ItemSchema](schemaResponse.get(0).toJson());
    }
    schemaDocument
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

  def insertDocument(collectionLink: String,
                     document: Document,
                     requestOptions: RequestOptions): Unit = {
    logTrace(s"Inserting document $document")
    documentClient.createDocument(collectionLink, document, requestOptions, false)
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

  private def getClientConfiguration(config: Config): ClientConfiguration = {
    // Generate connection policy
    val connectionPolicy = new ConnectionPolicy()

    connectionPolicy.setConnectionMode(connectionMode)

    val applicationName = config.get[String](CosmosDBConfig.ApplicationName)
    if (applicationName.isDefined) {
      // Merging the Spark connector version with Spark executor process id and application name for user agent
      connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix + " " + ManagementFactory.getRuntimeMXBean().getName() + " " + applicationName.get)
    } else {
      // Merging the Spark connector version with Spark executor process id for user agent
      connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix + " " + ManagementFactory.getRuntimeMXBean().getName())
    }

    config.get[String](CosmosDBConfig.ConnectionRequestTimeout) match {
      case Some(connectionRequestTimeoutStr) => connectionPolicy.setRequestTimeout(connectionRequestTimeoutStr.toInt)
      case None => // skip
    }

    config.get[String](CosmosDBConfig.ConnectionIdleTimeout) match {
      case Some(connectionIdleTimeoutStr) => connectionPolicy.setIdleConnectionTimeout(connectionIdleTimeoutStr.toInt)
      case None => // skip
    }

    val maxConnectionPoolSize = config.getOrElse[String](CosmosDBConfig.ConnectionMaxPoolSize, CosmosDBConfig.DefaultMaxConnectionPoolSize.toString)
    connectionPolicy.setMaxPoolSize(maxConnectionPoolSize.toInt)

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
