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

import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeCache
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

object CosmosDBConnectionCache extends CosmosDBLoggingTrait {
  private val nullRequestOptions: RequestOptions = null
  private lazy val clientCache
      : ConcurrentHashMap[ClientConfiguration, ClientCacheEntry] =
    new ConcurrentHashMap[ClientConfiguration, ClientCacheEntry]()
  // For verification purpose
  var lastConnectionPolicy: ConnectionPolicy = _
  var lastConsistencyLevel: Option[ConsistencyLevel] = _

  private lazy val createClientFunc =
    new java.util.function.Function[ClientConfiguration, ClientCacheEntry]() {
      override def apply(config: ClientConfiguration): ClientCacheEntry = {
        val client = if (config.resourceLink.isDefined) {
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
          s"Creating new ClientCacheEntry#${clientCacheEntry.hashCode()} " +
            s"for ClientConfiguration#${config.hashCode()}" +
            s"with DocumentClient#${client.hashCode()}"
        )
        clientCacheEntry
      }
    }

  private val bulkExecutorInitializationRetryOptions =
    createBulkExecutorInitializationRetryOptions()

  def invalidate(config: ClientConfiguration): Unit = {
    val oldClientCacheEntry: ClientCacheEntry = clientCache.remove(config)

    if (oldClientCacheEntry != null) {
      val bulkExecutorHash = if (oldClientCacheEntry.bulkExecutor.isDefined) {
        oldClientCacheEntry.bulkExecutor.hashCode()
      } else {
        "n/a"
      }

      logInfo(
        s"Invalidating ClientCacheEntry#${oldClientCacheEntry.hashCode()} " +
          s"for ClientConfiguration#${config.hashCode()}" +
          s"with DocumentClient#${oldClientCacheEntry.docClient.hashCode()} " +
          s"with BulkExecutor#$bulkExecutorHash"
      )
    }
  }

  def getOrCreateBulkExecutor(
      config: ClientConfiguration
  ): DocumentBulkExecutor = {
    val clientCacheEntry = clientCache.computeIfAbsent(config, createClientFunc)
    if (clientCacheEntry.bulkExecutor.isDefined) {
      clientCacheEntry.bulkExecutor.get
    } else {
      val client: DocumentClient = createClientFunc(config).docClient
      val pkDef = getPartitionKeyDefinition(config)
      val maxAvailableThroughput = getOrReadMaxAvailableThroughput(config)

      val effectivelyAvailableThroughputForBulkOperations =
        if (config.bulkConfig.maxThroughputForBulkOperations.isDefined) {
          Math.min(
            config.bulkConfig.maxThroughputForBulkOperations.get,
            maxAvailableThroughput
          )
        } else {
          maxAvailableThroughput
        }

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
        .withMaxMiniBatchSize(config.bulkConfig.maxMiniBatchImportSizeKB * 1024)

      // Instantiate DocumentBulkExecutor
      val bulkExecutor = builder.build()

      logInfo(
        s"Creating bulk executor for ClientCacheEntry#${clientCacheEntry.hashCode()} " +
          s"for ClientConfiguration#${config.hashCode()}" +
          s"with DocumentClient#${client.hashCode()}" +
          s"with BulkExecutor#${bulkExecutor.hashCode()}"
      )

      clientCache.replace(
        config,
        clientCacheEntry,
        clientCacheEntry.copy(bulkExecutor = Some(bulkExecutor))
      )

      bulkExecutor
    }
  }

  def reinitializeClient(config: ClientConfiguration): AnyVal = {
    val clientCacheEntry: ClientCacheEntry = clientCache.get(config)
    if (clientCacheEntry != null) {
      val containerMetadata: ContainerMetadata = getOrReadContainerMetadata(
        config
      )
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
      clientCache.replace(
        config,
        clientCacheEntry,
        clientCacheEntry.copy(docClient = docClient)
      )
    }
  }

  def getOrCreateClient(config: ClientConfiguration): DocumentClient = {
    logTrace(
      s"--> getOrCreateClient for ClientConfiguration#${config.hashCode()}, " +
        s"ClientCache#${clientCache.hashCode()} " +
        s"- record count ${clientCache.size()}"
    )
    val clientCacheEntry = clientCache.computeIfAbsent(config, createClientFunc)
    logTrace(
      s"<-- getOrCreateClient for ClientConfiguration#${config.hashCode()}, " +
        s"ClientCache#${clientCache.hashCode()} " +
        s" - record count ${clientCache.size()} " +
        s" with ClientCacheEntry#${clientCacheEntry.hashCode()}" +
        s" with DocumentClient#${clientCacheEntry.docClient.hashCode()}"
    )

    clientCacheEntry.docClient
  }

  def getPartitionKeyDefinition(
      config: ClientConfiguration
  ): PartitionKeyDefinition = {
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

  private def getOrReadMaxAvailableThroughput(
      config: ClientConfiguration
  ): Int = {
    val clientCacheEntry = clientCache.computeIfAbsent(config, createClientFunc)
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
        clientCache.replace(
          config,
          clientCacheEntry,
          clientCacheEntry.copy(maxAvailableThroughput =
            containerProvisionedThroughput
          )
        )

        logDebug(
          s"Read throughout '${containerProvisionedThroughput.get}' for " +
            s"collection '${containerMetadata.id}' with DocumentClient#" +
            s"${documentClient.hashCode()} and ContainerMetadata#${containerMetadata.hashCode()}"
        )

        containerProvisionedThroughput.get
      } else {
        val databaseMetadata: DatabaseMetadata = getOrReadDatabaseMetadata(config)
        val databaseProvisionedThroughput: Option[Int] =
          queryOffers(documentClient, databaseMetadata.resourceId)

        if (databaseProvisionedThroughput.isDefined) {
          clientCache.replace(
            config,
            clientCacheEntry,
            clientCacheEntry.copy(maxAvailableThroughput =
              databaseProvisionedThroughput
            )
          )

          logDebug(
            s"Read throughout '${databaseProvisionedThroughput.get}' for " +
              s"collection '${databaseMetadata.id}' with DocumentClient#${documentClient.hashCode()} " +
              s"and DatabaseMetadata#${databaseMetadata.hashCode()}"
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
    }

    val offer = offers.get(0)
    val collectionThroughput = if (offer.getString("offerVersion") == "V1") {
      CosmosDBConfig.SinglePartitionCollectionOfferThroughput
    } else {
      offer.getContent.getInt("offerThroughput")
    }

    Some(collectionThroughput)
  }

  def getOrReadContainerMetadata(
      config: ClientConfiguration
  ): ContainerMetadata = {
    val clientCacheEntry = clientCache.computeIfAbsent(config, createClientFunc)
    if (clientCacheEntry.containerMetadata.isDefined) {
      clientCacheEntry.containerMetadata.get
    } else {
      val documentClient: DocumentClient = clientCacheEntry.docClient
      val collectionLink: String =
        ClientConfiguration.getCollectionLink(config.database, config.container)
      val collection: DocumentCollection = documentClient
        .readCollection(collectionLink, nullRequestOptions)
        .getResource

      val containerMetadata: ContainerMetadata = ContainerMetadata(
        config.container,
        collection.getSelfLink,
        collection.getResourceId,
        collection.getPartitionKey
      )

      val newClientCacheEntry =
        clientCacheEntry.copy(containerMetadata = Some(containerMetadata))

      logTrace(
        s"Read container metadata '${containerMetadata.hashCode()}' " +
          s"for collection '${containerMetadata.id}' " +
          s"for ClientConfiguration#${config.hashCode()} " +
          s"for ClientCacheEntry#${clientCacheEntry.hashCode()} " +
          s"with DocumentClient#${documentClient.hashCode()}"
      )

      if (clientCache.replace(config, clientCacheEntry, newClientCacheEntry)) {
        logDebug(
          s"Updating container metadata '${containerMetadata.hashCode()}' " +
            s"for collection '${containerMetadata.id}' " +
            s"for ClientConfiguration#${config.hashCode()} " +
            s"for ClientCacheEntry#${clientCacheEntry.hashCode()} " +
            s"with DocumentClient#${documentClient.hashCode()}" +
            s"into new ClientCacheEntry#${newClientCacheEntry.hashCode()}"
        )
      }

      containerMetadata
    }
  }

  private def getOrReadDatabaseMetadata(
      config: ClientConfiguration
  ): DatabaseMetadata = {
    val clientCacheEntry = clientCache.computeIfAbsent(config, createClientFunc)
    if (clientCacheEntry.databaseMetadata.isDefined) {
      clientCacheEntry.databaseMetadata.get
    } else {
      val documentClient: DocumentClient = clientCacheEntry.docClient
      val databaseLink: String =
        ClientConfiguration.getDatabaseLink(config.database)
      val database: Database = documentClient
        .readDatabase(databaseLink, nullRequestOptions)
        .getResource

      val databaseMetadata: DatabaseMetadata =
        DatabaseMetadata(config.database, database.getResourceId)

      logDebug(
        s"Read database metadata '${databaseMetadata
          .hashCode()}' for database '${databaseMetadata.id}' " +
          s"with DocumentClient#${documentClient.hashCode()}"
      )

      clientCache.replace(
        config,
        clientCacheEntry,
        clientCacheEntry.copy(databaseMetadata = Some(databaseMetadata))
      )

      databaseMetadata
    }
  }

  private def createBulkExecutorInitializationRetryOptions(): RetryOptions = {
    val bulkExecutorInitializationRetryOptions = new RetryOptions()
    bulkExecutorInitializationRetryOptions
      .setMaxRetryAttemptsOnThrottledRequests(1000)
    bulkExecutorInitializationRetryOptions.setMaxRetryWaitTimeInSeconds(1000)

    bulkExecutorInitializationRetryOptions
  }

  private def createConnectionPolicy(
      settings: ConnectionPolicySettings
  ): ConnectionPolicy = {
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

  private def createClientWithMasterKey(
      config: ClientConfiguration
  ): DocumentClient = {
    lastConnectionPolicy = createConnectionPolicy(
      config.connectionPolicySettings
    )
    val consistencyLevel = ConsistencyLevel.valueOf(config.consistencyLevel)
    lastConsistencyLevel = Some(consistencyLevel)

    new DocumentClient(
      config.host,
      config.key,
      lastConnectionPolicy,
      consistencyLevel
    )
  }

  private def createClientWithResourceToken(
      config: ClientConfiguration
  ): DocumentClient = {
    val permissionWithNamedResourceLink = new Permission()
    permissionWithNamedResourceLink.set("_token", config.key)
    permissionWithNamedResourceLink.setResourceLink(config.resourceLink.get)

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

    // Grab the other permission thing
    val collection = client.readCollection(config.resourceLink.get, null)
    client.close()

    val collectionSelfLink = collection.getResource.getSelfLink

    val permissionWithSelfLink = new Permission()
    permissionWithSelfLink.set("_token", config.key)
    permissionWithSelfLink.setResourceLink(collectionSelfLink)

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
