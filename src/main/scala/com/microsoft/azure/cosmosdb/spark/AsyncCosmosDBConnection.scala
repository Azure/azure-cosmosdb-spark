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

import java.util.concurrent.TimeUnit

import com.microsoft.azure.cosmosdb.spark.config._
import rx.Observable
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.internal._
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag


private[spark] case class AsyncCosmosDBConnection(config: Config) extends LoggingTrait with Serializable {

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

  private val databaseName = config.get[String](CosmosDBConfig.Database).get
  private val collectionName = config.get[String](CosmosDBConfig.Collection).get
  val collectionLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName/${Paths.COLLECTIONS_PATH_SEGMENT}/$collectionName"
  private val connectionMode = ConnectionMode.valueOf(config.get[String](CosmosDBConfig.ConnectionMode)
    .getOrElse(CosmosDBConfig.DefaultConnectionMode))

  @transient private var asyncClient: AsyncDocumentClient = _

  def importWithRxJava[D: ClassTag](iter: Iterator[D],
                                    connection: AsyncCosmosDBConnection,
                                    writingBatchSize: Integer,
                                    writingBatchDelayMs: Long,
                                    rootPropertyToSave: Option[String],
                                    upsert: Boolean): Unit = {

    var observables = new java.util.ArrayList[Observable[ResourceResponse[Document]]](writingBatchSize)
    var createDocumentObs: Observable[ResourceResponse[Document]] = null
    var batchSize = 0
    iter.foreach(item => {
      val document: Document = item match {
        case doc: Document => doc
        case row: Row =>
          if (rootPropertyToSave.isDefined) {
            new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
          } else {
            new Document(CosmosDBRowConverter.rowToJSONObject(row).toString())
          }
        case any => new Document(any.toString)
      }
      logDebug(s"Inserting document $document")
      if (upsert)
        createDocumentObs = connection.upsertDocument(document, null)
      else
        createDocumentObs = connection.createDocument(document, null)
      observables.add(createDocumentObs)
      batchSize = batchSize + 1
      if (batchSize % writingBatchSize == 0) {
        Observable.merge(observables).toBlocking.last()
        if (writingBatchDelayMs > 0) {
          TimeUnit.MILLISECONDS.sleep(writingBatchDelayMs)
        }
        observables.clear()
        batchSize = 0
      }
    })
    if (!observables.isEmpty) {
      Observable.merge(observables).toBlocking.last()
    }
  }

  def upsertDocument(document: Document,
                     requestOptions: RequestOptions): Observable[ResourceResponse[Document]] = {
    logTrace(s"Upserting document $document")
    asyncDocumentClient.upsertDocument(collectionLink, document, requestOptions, false)
  }

  def createDocument(document: Document,
                     requestOptions: RequestOptions): Observable[ResourceResponse[Document]] = {
    logTrace(s"Creating document $document")
    asyncDocumentClient.createDocument(collectionLink, document, requestOptions, false)
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
      case Some(connectionIdleTimeoutStr) => connectionPolicy.setIdleConnectionTimeoutInMillis(connectionIdleTimeoutStr.toInt)
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

  case class ClientConfiguration(host: String,
                                 key: String,
                                 connectionPolicy: ConnectionPolicy,
                                 consistencyLevel: ConsistencyLevel)

}
