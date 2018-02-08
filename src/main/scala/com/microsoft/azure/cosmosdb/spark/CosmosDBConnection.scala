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

import rx.Observable
import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.internal._
import com.microsoft.azure.documentdb.rx._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

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
  private val connectionMode = ConnectionMode.valueOf(config.get[String](CosmosDBConfig.ConnectionMode)
    .getOrElse(CosmosDBConfig.DefaultConnectionMode))
  private var collection: DocumentCollection = _
  val collectionLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName/${Paths.COLLECTIONS_PATH_SEGMENT}/$collectionName"

  @transient private var client: DocumentClient = _

  @transient private var asyncClient: AsyncDocumentClient = _

  @transient private var bulkImporter: DocumentBulkExecutor = _

  private lazy val documentClient: DocumentClient = {
    if (client == null) {
      client = accquireClient(connectionMode)

      CosmosDBConnection.lastConnectionPolicy = client.getConnectionPolicy
    }
    client
  }

  private lazy val asyncDocumentClient: AsyncDocumentClient = {
    if (asyncClient == null) {
      this.synchronized {
        if (asyncClient == null) {
          val clientConfig = getClientConfiguration(config)

          asyncClient = new AsyncDocumentClient
          .Builder()
            .withServiceEndpoint(clientConfig.host)
            .withMasterKey(clientConfig.key)
            .withConnectionPolicy(clientConfig.connectionPolicy)
            .withConsistencyLevel(clientConfig.consistencyLevel)
            .build
        }
      }
    }

    asyncClient
  }

  def getDocumentBulkImporter(collectionThroughput: Int): DocumentBulkExecutor = {
    if (bulkImporter == null) {
      bulkImporter = DocumentBulkExecutor.builder.from(documentClient,
        databaseName,
        collectionName,
        getCollection.getPartitionKey,
        collectionThroughput
      ).build()
    }

    bulkImporter
  }

  private def getClientConfiguration(config: Config): ClientConfiguration = {
    val connectionPolicy = new ConnectionPolicy()
    connectionPolicy.setConnectionMode(connectionMode)
    connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix)
    config.get[String](CosmosDBConfig.ConnectionMaxPoolSize) match {
      case Some(maxPoolSizeStr) => connectionPolicy.setMaxPoolSize(maxPoolSizeStr.toInt)
      case None => // skip
    }
    config.get[String](CosmosDBConfig.ConnectionIdleTimeout) match {
      case Some(connectionIdleTimeoutStr) => connectionPolicy.setIdleConnectionTimeout(connectionIdleTimeoutStr.toInt)
      case None => // skip
    }
    val maxRetryAttemptsOnThrottled = config.get[String](CosmosDBConfig.QueryMaxRetryOnThrottled)
    if (maxRetryAttemptsOnThrottled.isDefined) {
      connectionPolicy.getRetryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttemptsOnThrottled.get.toInt)
    }
    val maxRetryWaitTimeSecs = config.get[String](CosmosDBConfig.QueryMaxRetryWaitTimeSecs)
    if (maxRetryWaitTimeSecs.isDefined) {
      connectionPolicy.getRetryOptions.setMaxRetryWaitTimeInSeconds(maxRetryWaitTimeSecs.get.toInt)
    }
    val consistencyLevel = ConsistencyLevel.valueOf(config.get[String](CosmosDBConfig.ConsistencyLevel)
      .getOrElse(CosmosDBConfig.DefaultConsistencyLevel))

    val option = config.get[String](CosmosDBConfig.PreferredRegionsList)

    if (option.isDefined) {
      logWarning(s"CosmosDBConnection::Input preferred region list: ${option.get}")
      val preferredLocations = option.get.split(";").toSeq.map(_.trim)
      connectionPolicy.setPreferredLocations(preferredLocations)
    }

    val bulkimport = config.get[String](CosmosDBConfig.BulkImport).
      getOrElse(CosmosDBConfig.DefaultBulkImport.toString).
      toBoolean
    if (bulkimport) {
      // The bulk import library handles the throttling requests on its own
      // Gateway connection mode needed to avoid potential master partition throttling
      // as the number of tasks grow larger for collection with a lot of partitions.
      connectionPolicy.getRetryOptions.setMaxRetryAttemptsOnThrottledRequests(0)
      connectionPolicy.setConnectionMode(ConnectionMode.Gateway)
    }

    ClientConfiguration(
      config.get[String](CosmosDBConfig.Endpoint).get,
      config.get[String](CosmosDBConfig.Masterkey).get,
      connectionPolicy,
      consistencyLevel
    )
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
    val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    feedResponse.getQueryIterable.iterator()
  }

  def queryDocuments (collectionLink: String, queryString : String,
                      feedOpts : FeedOptions) : Iterator [Document] = {
    val feedResponse = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    feedResponse.getQueryIterable.iterator()
  }

  def readDocuments(feedOptions: FeedOptions): Iterator[Document] = {
    documentClient.readDocuments(collectionLink, feedOptions).getQueryIterable.iterator()
  }

  def readChangeFeed(changeFeedOptions: ChangeFeedOptions, isStreaming: Boolean): Tuple2[Iterator[Document], String] = {
    val feedResponse = documentClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)
    if (isStreaming) {
      // In streaming scenario, the change feed need to be materialized in order to get the information of the continuation token
      val cfDocuments: ListBuffer[Document] = new ListBuffer[Document]
      while (feedResponse.getQueryIterator.hasNext) {
        val feedItems = feedResponse.getQueryIterable.fetchNextBlock()
        cfDocuments.addAll(feedItems)
        logDebug(s"Receving change feed items ${if (feedItems.nonEmpty) feedItems(0)}")
      }
      Tuple2.apply(cfDocuments.iterator(), feedResponse.getResponseContinuation)
    } else {
      Tuple2.apply(feedResponse.getQueryIterator, feedResponse.getResponseContinuation)
    }
  }

  def getCollection: DocumentCollection = {
    if (collection == null) {
      collection = documentClient.readCollection(collectionLink, null).getResource
    }
    collection
  }

  def upsertDocument(document: Document,
                     requestOptions: RequestOptions): Observable[ResourceResponse[Document]] = {
    logTrace(s"Upserting document $document")
    asyncDocumentClient.upsertDocument(collectionLink, document, requestOptions, false)
  }

  def upsertDocument(collectionLink: String,
                      document: Document,
                      requestOptions: RequestOptions): Unit = {
    logTrace(s"Upserting document $document")
    documentClient.upsertDocument(collectionLink, document, requestOptions, false)
  }

  def createDocument(document: Document,
                     requestOptions: RequestOptions): Observable[ResourceResponse[Document]] = {
    logTrace(s"Creating document $document")
    asyncDocumentClient.createDocument(collectionLink, document, requestOptions, false)
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
}
