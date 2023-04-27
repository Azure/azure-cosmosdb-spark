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

import java.util.concurrent.ConcurrentHashMap
import java.util.{Timer, TimerTask, UUID}

import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeCache
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

/**
  * Helper class that allows access to the resources used in CosmosDBConnection
  * that are cached as singletons on executors.
  */
object CosmosDBConnectionCache extends CosmosDBLoggingTrait {
  private lazy val clientCache: ConcurrentHashMap[ClientConfiguration, ClientCacheEntry] = 
    new ConcurrentHashMap[ClientConfiguration, ClientCacheEntry]()
  private lazy val createClientFunc =
    new java.util.function.Function[ClientConfiguration, ClientCacheEntry]() {
      override def apply(config: ClientConfiguration): ClientCacheEntry = {
        val client = if (config.authConfig.resourceLink.isDefined) {
          createClientWithResourceToken(config)
        } else {
          createClientWithMasterKey(config)
        }

        val clientCacheEntry = ClientCacheEntry(
          client,
          bulkExecutor = None,
          containerMetadata = None,
          databaseMetadata = None,
          maxAvailableThroughput = None
        )

        logInfo(
          s"Creating new ${clientCacheEntry.getLogMessage}"
        )
        clientCacheEntry
      }
    }
  
  private val rnd = scala.util.Random

  private val refreshDelay : Long = (10 * 60 * 1000) + rnd.nextInt(5 * 60 * 1000) // in 10 - 15 minutes
  private val refreshPeriod : Long = 15 * 60 * 1000 // every 15 minutes
  // main purpose of the time is to allow bulk operations to consume
  // additional throughput when more RUs are getting provisioned
  private val timerName = "throughput-refresh-timer"
  private val timer: Timer = new Timer(timerName, true)

  private val nullRequestOptions: RequestOptions = null
  private val bulkExecutorInitializationRetryOptions: RetryOptions = {
    val bulkExecutorInitializationRetryOptions = new RetryOptions()
    bulkExecutorInitializationRetryOptions
      .setMaxRetryAttemptsOnThrottledRequests(1000)
    bulkExecutorInitializationRetryOptions.setMaxRetryWaitTimeInSeconds(1000)

    bulkExecutorInitializationRetryOptions
  }
  // For verification purpose
  var lastConnectionPolicy: ConnectionPolicy = _
  var lastConsistencyLevel: Option[ConsistencyLevel] = _

  startRefreshTimer()

  def purgeCache(config: ClientConfiguration) : Unit = {
    /*
    * Resets the Connection Cache - this will be triggered if the container
    * cannot be found - which might happen after deleting/re-creating the container
    * with the same name
    *
    * @param config The cache key - represents the config settings
    */
    clientCache.remove(config)
  }

  def getOrCreateBulkExecutor(config: ClientConfiguration): DocumentBulkExecutor = {
    val clientCacheEntry = getOrCreateClientCacheEntry(config)
    if (clientCacheEntry.bulkExecutor.isDefined) {
      clientCacheEntry.bulkExecutor.get
    } else {
      val client: DocumentClient = clientCacheEntry.docClient
      val pkDef = getPartitionKeyDefinition(config)

      val effectivelyAvailableThroughputForBulkOperations = getOrReadMaxAvailableThroughput(config)

      val builder = DocumentBulkExecutor.builder
        .from(
          client,
          config.database,
          config.container,
          pkDef,
          effectivelyAvailableThroughputForBulkOperations
        )
        .withInitializationRetryOptions(bulkExecutorInitializationRetryOptions)
        .withMaxUpdateMiniBatchCount(config.bulkConfig.maxMiniBatchUpdateCount)

      // Instantiate DocumentBulkExecutor
      val bulkExecutor = builder.build()

      attemptClientCacheEntryUpdate(
        config,
        clientCacheEntry,
        operationName = "BulkExecutor",
        oldClientCacheEntry => oldClientCacheEntry.copy(bulkExecutor = Some(bulkExecutor))
      )

      bulkExecutor
    }
  }

  def getPartitionKeyDefinition(config: ClientConfiguration): PartitionKeyDefinition = {
    config.bulkConfig.partitionKeyOption match {
      case None =>
        val containerMetadata: ContainerMetadata = getOrReadContainerMetadata(
          config
        )
        containerMetadata.partitionKeyDefinition
      case Some(pk) =>
        val pkDefinition = new PartitionKeyDefinition()
        val paths: ListBuffer[String] = new ListBuffer[String]()
        paths.add(pk)
        pkDefinition.setPaths(paths)
        pkDefinition
    }
  }

  /**
    * Resets the partition key range cache within the Document client
    * This is done to enforce that the DocumentClient retrieves fresh partition key range
    * metadata - for example when computing the number of partitions to execute
    * queries on etc.
    *
    * @param config The cache key - represents the config settings
    */
  def reinitializeClient(config: ClientConfiguration): Unit = {
    val clientCacheEntry: ClientCacheEntry = clientCache.get(config)
    if (clientCacheEntry != null) {
      val containerMetadata: ContainerMetadata = getOrReadContainerMetadata(config)
      val docClient: DocumentClient = clientCacheEntry.docClient
      logDebug(
        s"Updating Partition key range cache for DocumentClient#${docClient.hashCode()} " +
          s"of ClientConfiguration#${config.hashCode()}"
      )
      val field = docClient.getClass.getDeclaredField("partitionKeyRangeCache")
      field.setAccessible(true)
      val partitionKeyRangeCache =
        field.get(docClient).asInstanceOf[PartitionKeyRangeCache]
      val range =
        new com.microsoft.azure.documentdb.internal.routing.Range[String](
          "00",
          "FF",
          true,
          true
        )

      partitionKeyRangeCache.getOverlappingRanges(
        containerMetadata.selfLink,
        range,
        true
      )
    }
  }

  def getOrReadContainerMetadata(config: ClientConfiguration): ContainerMetadata = {
    val clientCacheEntry = getOrCreateClientCacheEntry(config)
    if (clientCacheEntry.containerMetadata.isDefined) {
      clientCacheEntry.containerMetadata.get
    } else {
      val documentClient: DocumentClient = clientCacheEntry.docClient
      val collectionLink: String =
        config.getCollectionLink()
      val collection: DocumentCollection = documentClient
        .readCollection(collectionLink, nullRequestOptions)
        .getResource

      val containerMetadata: ContainerMetadata = ContainerMetadata(
        config.container,
        collection.getSelfLink,
        collection.getResourceId,
        collection.getPartitionKey
      )

      attemptClientCacheEntryUpdate(
        config,
        clientCacheEntry,
        operationName = "ContainerMetadata",
        oldClientCacheEntry => oldClientCacheEntry.copy(containerMetadata = Some(containerMetadata))
      )

      containerMetadata
    }
  }

  private def startRefreshTimer() : Unit = {
    logInfo(s"$timerName: scheduling timer - delay: $refreshDelay ms, period: $refreshPeriod ms")
    timer.schedule(
      new TimerTask { def run(): Unit = onRunRefreshTimer() },
      refreshDelay, 
      refreshPeriod)
  }

  private def onRunRefreshTimer() : Unit = {
    val sequentialParallelismThreshold = java.lang.Long.MAX_VALUE
    logTrace(s"--> $timerName: onRefreshTimer")
    
    val foreachConsumer = new java.util.function.Consumer[ClientConfiguration]() {
      override def accept(config: ClientConfiguration): Unit = {

        // The throughput available for bulk operations can change
        // if it gets reduced this would just result in higher number of
        // throttled requests - no big deal
        // but when adding provisioned throughput the bulk ingestion wouldn't pick-up
        // this change - to change this we reset the bulk executor and maxAvailableThroughput
        // from the cache entry - so any additional will get picked-up sooner
        val refreshEntryFunc = new java.util.function.BiFunction[ClientConfiguration, ClientCacheEntry, ClientCacheEntry]() {
          override def apply(key: ClientConfiguration, oldClientCacheEntry: ClientCacheEntry) : ClientCacheEntry = {
            val newClientCacheEntry = oldClientCacheEntry.copy(
              bulkExecutor = None,
              maxAvailableThroughput = None
            )

            logDebug(s"$timerName: ClientConfiguration#${config.hashCode} has been reset - new " +
                s"${newClientCacheEntry.getLogMessage}, previously ${oldClientCacheEntry.getLogMessage}")

            newClientCacheEntry
          }
        }

        clientCache.computeIfPresent(config, refreshEntryFunc)
      }
    }

    clientCache.forEachKey(
      sequentialParallelismThreshold,
      foreachConsumer)
    
    logTrace(s"<-- $timerName: onRefreshTimer")
  }

  private def attemptClientCacheEntryUpdate(
                                             configuration: ClientConfiguration,
                                             oldClientCacheEntry: ClientCacheEntry,
                                             operationName: String,
                                             updateFunc: ClientCacheEntry => ClientCacheEntry) = {

    logTrace(
      s"Creating $operationName for ${oldClientCacheEntry.getLogMessage}"
    )

    val newClientCacheEntry = updateFunc(oldClientCacheEntry)

    // Intentionally using replace here - not computeIfPresent for example
    // Calculating the "update" often involves IO - for example to calculate the throughput
    // doing it within computeIfPresent would basically do this under a lock
    // instead we risk that teh IO operations are done multiple times
    // and instead only one of the threads "wins" - the concurrency in Executors
    // is small enough that this won't be a common pattern - so is a reasonable trade-off
    if (clientCache.replace(configuration, oldClientCacheEntry, newClientCacheEntry)) {
      logInfo(
        s"Updated $operationName for ${newClientCacheEntry.getLogMessage}"
      )
    }

    newClientCacheEntry
  }

  private def getOrCreateClientCacheEntry(config: ClientConfiguration): ClientCacheEntry = {
    clientCache.computeIfAbsent(config, createClientFunc)
  }

  def getOrCreateClient(config: ClientConfiguration): DocumentClient = {
    logTrace(
      s"--> getOrCreateClient for ClientConfiguration#${config.hashCode()}, " +
        s"ClientCache#${clientCache.hashCode()} " +
        s"- record count ${clientCache.size()}"
    )
    val clientCacheEntry = getOrCreateClientCacheEntry(config)
    logTrace(
      s"<-- getOrCreateClient for ClientConfiguration#${config.hashCode()}, " +
        s"ClientCache#${clientCache.hashCode()} " +
        s" - record count ${clientCache.size()} " +
        s" returns ${clientCacheEntry.getLogMessage}"
    )

    clientCacheEntry.docClient
  }

  private def getOrReadMaxAvailableThroughput(config: ClientConfiguration): Int = {
    val clientCacheEntry = getOrCreateClientCacheEntry(config)
    if (clientCacheEntry.maxAvailableThroughput.isDefined) {
      clientCacheEntry.maxAvailableThroughput.get
    } else {
      val documentClient: DocumentClient = clientCacheEntry.docClient
      val containerMetadata: ContainerMetadata = getOrReadContainerMetadata(
        config
      )
      val containerProvisionedThroughput: Option[Int] =
        queryOffers(documentClient, containerMetadata.resourceId)

      if (containerProvisionedThroughput.isDefined) {
        attemptClientCacheEntryUpdate(
          config,
          clientCacheEntry,
          operationName = "MaxAvailableThroughput-Container",
          oldClientCacheEntry => oldClientCacheEntry.copy(maxAvailableThroughput = containerProvisionedThroughput)
        )

        containerProvisionedThroughput.get
      } else {
        val databaseMetadata: DatabaseMetadata = getOrReadDatabaseMetadata(config)
        val databaseProvisionedThroughput: Option[Int] =
          queryOffers(documentClient, databaseMetadata.resourceId)

        if (databaseProvisionedThroughput.isDefined) {
          attemptClientCacheEntryUpdate(
            config,
            clientCacheEntry,
            operationName = "MaxAvailableThroughput-Database",
            oldClientCacheEntry => oldClientCacheEntry.copy(maxAvailableThroughput = databaseProvisionedThroughput)
          )

          databaseProvisionedThroughput.get
        } else {
          throw new IllegalStateException(
            "Cannot find the collection corresponding offer.")
        }
      }
    }
  }

  private def queryOffers(
                           documentClient: DocumentClient,
                           offerResourceId: String
                         ): Option[Int] = {
    val parameters = new SqlParameterCollection()
    parameters.add(new SqlParameter("@offerResourceId", offerResourceId))

    val query = new SqlQuerySpec(
      "SELECT * FROM c where c.offerResourceId = @offerResourceId",
      parameters
    )

    val nullFeedOptions = null
    val offers =
      documentClient.queryOffers(query, nullFeedOptions).getQueryIterable.toList

    if (offers.isEmpty) {
      None
    } else {
      val offer = offers.get(0)
      val collectionThroughput = if (offer.getString("offerVersion") == "V1") {
        CosmosDBConfig.SinglePartitionCollectionOfferThroughput
      } else {
        offer.getContent.getInt("offerThroughput")
      }

      Some(collectionThroughput)
    }
  }

  private def getOrReadDatabaseMetadata(config: ClientConfiguration): DatabaseMetadata = {
    val clientCacheEntry = getOrCreateClientCacheEntry(config)
    if (clientCacheEntry.databaseMetadata.isDefined) {
      clientCacheEntry.databaseMetadata.get
    } else {
      val documentClient: DocumentClient = clientCacheEntry.docClient
      val databaseLink: String =
        config.getDatabaseLink()
      val database: Database = documentClient
        .readDatabase(databaseLink, nullRequestOptions)
        .getResource

      val databaseMetadata: DatabaseMetadata =
        DatabaseMetadata(config.database, database.getResourceId)

      attemptClientCacheEntryUpdate(
        config,
        clientCacheEntry,
        operationName = "ContainerMetadata",
        oldClientCacheEntry => oldClientCacheEntry.copy(databaseMetadata = Some(databaseMetadata))
      )

      databaseMetadata
    }
  }

  private def createClientWithMasterKey(config: ClientConfiguration): DocumentClient = {
    lastConnectionPolicy = createConnectionPolicy(
      config.connectionPolicySettings
    )
    val consistencyLevel = ConsistencyLevel.valueOf(config.consistencyLevel)
    lastConsistencyLevel = Some(consistencyLevel)

    var client = new DocumentClient(
      config.host,
      config.authConfig.authKey,
      lastConnectionPolicy,
      consistencyLevel
    )

    client = config.getQueryLoggingPath() match {
      case Some(path) => {
        val logger = new HdfsLogWriter(
          config.queryLoggingCorrelationId.getOrElse(""),
          config.hadoopConfig.toMap,
          path)

        client.setLogWriter(logger);
      }
      case None => client
    }

    client = config.getCountLoggingPath() match {
      case Some(path) => {
        val logger = new HdfsLogWriter(
          config.queryLoggingCorrelationId.getOrElse(""),
          config.hadoopConfig.toMap,
          path)

        client.setCountLogWriter(logger);
      }
      case None => client
    }

    client
  }

  private def createConnectionPolicy(settings: ConnectionPolicySettings): ConnectionPolicy = {
    val connectionPolicy = new ConnectionPolicy()

    val connectionMode = ConnectionMode.valueOf(settings.connectionMode)
    connectionPolicy.setConnectionMode(connectionMode)
    connectionPolicy.setUserAgentSuffix(settings.userAgentSuffix)

    if (settings.requestTimeout.isDefined) {
      connectionPolicy.setRequestTimeout(settings.requestTimeout.get)
    }

    if (settings.connectionIdleTimeout.isDefined) {
      connectionPolicy.setIdleConnectionTimeout(
        settings.connectionIdleTimeout.get
      )
    }

    connectionPolicy.setMaxPoolSize(settings.maxPoolSize)
    connectionPolicy.getRetryOptions
      .setMaxRetryAttemptsOnThrottledRequests(
        settings.maxRetryAttemptsOnThrottled
      )
    connectionPolicy.getRetryOptions
      .setMaxRetryWaitTimeInSeconds(settings.maxRetryWaitTimeSecs)

    val preferredRegionsList = settings.preferredRegions
    if (preferredRegionsList.isDefined) {
      logTrace(
        s"CosmosDBConnection::Input preferred region list: ${preferredRegionsList.get}"
      )
      val preferredLocations =
        preferredRegionsList.get.split(";").map(_.trim).toList
      connectionPolicy.setPreferredLocations(preferredLocations)
    }

    connectionPolicy
  }

  private def createClientWithResourceToken(config: ClientConfiguration): DocumentClient = {
    val permissionWithNamedResourceLink = new Permission()
    permissionWithNamedResourceLink.set("_token", config.authConfig.authKey)
    permissionWithNamedResourceLink.setResourceLink(config.authConfig.resourceLink.get)

    val namedResourceLinkPermission =
      List[Permission](permissionWithNamedResourceLink)

    lastConnectionPolicy = createConnectionPolicy(
      config.connectionPolicySettings
    )
    val consistencyLevel = ConsistencyLevel.valueOf(config.consistencyLevel)
    lastConsistencyLevel = Some(consistencyLevel)

    val client = new DocumentClient(
      config.host,
      namedResourceLinkPermission,
      lastConnectionPolicy,
      consistencyLevel
    )

    // The namedResourceLinkPermission above provides the necessary
    // authentication information (resource token + link in the form of /dbs/<DBName>[/colls/<ContainerName>])
    // Internally the SDK can also address resources via the self-link (using resource-id instead of teh name,
    // which is unique and changes for each instance of a container for example when dropping and recreating
    // containers with the same name. The code below retrieves the self-link of the target container 
    // to construct the resource-id based permission.
    val collection = client.readCollection(config.authConfig.resourceLink.get, null)
    client.close()

    val collectionSelfLink = collection.getResource.getSelfLink

    val permissionWithSelfLink = new Permission()
    permissionWithSelfLink.set("_token", config.authConfig.authKey)
    permissionWithSelfLink.setResourceLink(collectionSelfLink)

    // adding both permissions - for name and resource-id based addresses
    val permissions =
      List[Permission](permissionWithSelfLink, permissionWithNamedResourceLink)

    new DocumentClient(
      config.host,
      permissions,
      lastConnectionPolicy,
      consistencyLevel
    )
  }
}
