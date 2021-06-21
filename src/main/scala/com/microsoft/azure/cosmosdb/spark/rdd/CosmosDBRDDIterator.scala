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
package com.microsoft.azure.cosmosdb.spark.rdd

import java.util
import java.util.Collections
import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes
import com.microsoft.azure.cosmosdb.internal.directconnectivity.ServiceUnavailableException
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.partitioner.CosmosDBPartition
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.cosmosdb.spark.{CosmosDBConnection, CosmosDBLoggingTrait}
import com.microsoft.azure.cosmosdb.spark.ContinuationTokenTrackingIterator
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.internal.HttpConstants.SubStatusCodes
import org.apache.commons.lang3.StringUtils
import org.apache.spark._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.TaskCompletionListener

import scala.collection.mutable
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat

object CosmosDBRDDIterator {

  val formatterGMT = ISODateTimeFormat.dateTime().withZone(DateTimeZone.forID("GMT"))

  // For verification purpose
  var lastFeedOptions: FeedOptions = _
  var hdfsUtils: HdfsUtils = _

  def initializeHdfsUtils(hadoopConfig: Map[String, String], changeFeedCheckpointLocation: String): Any = {
    if (hdfsUtils == null) {
      this.synchronized {
        if (hdfsUtils == null) {
          hdfsUtils = HdfsUtils(hadoopConfig, changeFeedCheckpointLocation)
        }
      }
    }
  }

  /**
    * Get the path to the next continuation token
    * @param queryName name of the query
    * @return
    */
  def getNextTokenPath(queryName: String): String = {
    queryName + queryName.hashCode + queryName.hashCode.hashCode()
  }

  /**
    * Get the next global continuation token for the collection in the provided config
    * @param config a structured stream configuration with connection details, a query name and a collection name
    * @return       the corresponding global continuation token
    */
  def getCollectionTokens(config: Config, shouldGetCurrentToken: Boolean = false): String = {
    val connection = CosmosDBConnection(config)
    val collectionLink = connection.getCollectionLink
    val queryName = config
      .get[String](CosmosDBConfig.ChangeFeedQueryName).get
    var tokenString: String = null

    // Construct a map of continuation tokens for the collection Rid
    val changeFeedCheckpointLocation: String = config
      .get[String](CosmosDBConfig.ChangeFeedCheckpointLocation)
      .getOrElse(StringUtils.EMPTY)
    var nextTokenMap: util.HashMap[String, String] = null
    if (!changeFeedCheckpointLocation.isEmpty) {
      nextTokenMap = CosmosDBRDDIterator.hdfsUtils.readChangeFeedToken(
        changeFeedCheckpointLocation,
        if (shouldGetCurrentToken) queryName else getNextTokenPath(queryName),
        if (shouldGetCurrentToken) getNextTokenPath(queryName) else queryName,
        collectionLink)
    }

    val streamingSlowSourceDelayMs: Int = config
      .get[String](CosmosDBConfig.StreamingSlowSourceDelayMs)
      .getOrElse(CosmosDBConfig.DefaultStreamingSlowSourceDelayMs.toString)
      .toInt

    if (nextTokenMap != null && !nextTokenMap.isEmpty) {
      // Add a timestamp entry in order to trigger the query for slow source scenario
      // This time token entry is not used to determine whether change feed data should be fetched in CosmosDBSource.getBatch
      nextTokenMap.put(
        CosmosDBConfig.StreamingTimestampToken,
        (System.currentTimeMillis() / streamingSlowSourceDelayMs).toString)

      tokenString = new ObjectMapper().writeValueAsString(nextTokenMap)
    }
    else {
      // Encoding offset as serialized empty map and not null to prevent serialization failure
      tokenString = new ObjectMapper().writeValueAsString(new ConcurrentHashMap[String, String]())
    }

    tokenString
  }

  /**
    * Used for verification purpose only. Clear the next continuation tokens cache to simulate a fresh start.
    */
  def resetCollectionContinuationTokens(): Any = {
    // no op
  }
}

class CosmosDBRDDIterator(hadoopConfig: mutable.Map[String, String],
                          taskContext: TaskContext,
                          partition: CosmosDBPartition,
                          config: Config,
                          maxItems: Option[Long],
                          requiredColumns: Array[String],
                          filters: Array[Filter])
  extends Iterator[Document]
    with CosmosDBLoggingTrait {

  val changeFeedCheckpointLocation: String = config
    .get[String](CosmosDBConfig.ChangeFeedCheckpointLocation)
    .getOrElse(StringUtils.EMPTY)

  CosmosDBRDDIterator.initializeHdfsUtils(hadoopConfig.toMap, changeFeedCheckpointLocation)

  // The continuation token for the target CosmosDB partition
  private var cfCurrentToken: String = _
  private var cfNextToken: String = _

  private var closed = false
  private var initialized = false
  private var itemCount: Long = 0
  private var retryCount: Int = 0
  private val maxRetryCountOnServiceUnavailable: Int = 100
  private val rnd = scala.util.Random

  lazy val reader: Iterator[Document] = {
    initialized = true
    val connection: CosmosDBConnection = CosmosDBConnection(config)

    val readingChangeFeed: Boolean = config
      .get[String](CosmosDBConfig.ReadChangeFeed)
      .getOrElse(CosmosDBConfig.DefaultReadChangeFeed.toString)
      .toBoolean

    val pageSize: Int = config
      .get[String](CosmosDBConfig.QueryPageSize)
      .getOrElse(CosmosDBConfig.DefaultPageSize.toString)
      .toInt

    /**
      * Query documents from CosmosDB
      */
    def queryDocuments: Iterator[Document] = {
      val feedOpts = new FeedOptions()
      feedOpts.setPageSize(pageSize)
      val maxDegreeOfParallelism = config
        .get[String](CosmosDBConfig.QueryMaxDegreeOfParallelism)
        .getOrElse(CosmosDBConfig.DefaultQueryMaxDegreeOfParallelism.toString)
        .toInt
      feedOpts.setMaxDegreeOfParallelism(maxDegreeOfParallelism)
      val bufferedItemCount = config
        .get[String](CosmosDBConfig.QueryMaxBufferedItemCount)
        .getOrElse(CosmosDBConfig.DefaultQueryMaxBufferedItemCount.toString)
        .toInt
      feedOpts.setMaxBufferedItemCount(bufferedItemCount)
      val enableScanInQuery = config
        .get[String](CosmosDBConfig.QueryEnableScan)
      if (enableScanInQuery.isDefined) {
        feedOpts.setEnableScanInQuery(enableScanInQuery.get.toBoolean)
      }
      val disableRUPerMinuteUsage = config
        .get[String](CosmosDBConfig.QueryDisableRUPerMinuteUsage)
      if (disableRUPerMinuteUsage.isDefined) {
        feedOpts.setDisableRUPerMinuteUsage(disableRUPerMinuteUsage.get.toBoolean)
      }
      val emitVerboseTraces = config
        .get[String](CosmosDBConfig.QueryEmitVerboseTraces)
      if (emitVerboseTraces.isDefined) {
        feedOpts.setEmitVerboseTracesInQuery(emitVerboseTraces.get.toBoolean)
      }
      val responseContinuationTokenLimitInKb = config
        .get[String](CosmosDBConfig.ResponseContinuationTokenLimitInKb)
        .getOrElse(CosmosDBConfig.DefaultResponseContinuationTokenLimitInKb.toString)
        .toInt
      feedOpts.setResponseContinuationTokenLimitInKb(responseContinuationTokenLimitInKb)

      feedOpts.setPartitionKeyRangeIdInternal(partition.partitionKeyRangeId.toString)
      CosmosDBRDDIterator.lastFeedOptions = feedOpts

      val queryString = config
        .get[String](CosmosDBConfig.QueryCustom)
        .getOrElse(FilterConverter.createQueryString(requiredColumns, filters))
      logInfo(s"CosmosDBRDDIterator::LazyReader, created query string: $queryString")

      var iteratorDocument: Iterator[Document] = Iterator()

      if (queryString == FilterConverter.defaultQuery) {
        // If there is no filters, read feed should be used
        iteratorDocument = connection.readDocuments(feedOpts)
      } else {
        iteratorDocument = connection.queryDocuments(queryString, feedOpts)
      }
      iteratorDocument
    }

    /**
      * Read documents change feed
      */
    def readChangeFeed: Iterator[Document] = {

      val objectMapper: ObjectMapper = new ObjectMapper()

      val queryName: String = config
        .get[String](CosmosDBConfig.ChangeFeedQueryName)
        .get
      val partitionId = partition.partitionKeyRangeId.toString
      var parentPartitionId = ""

      // Get the latest parents' partitionId when the child partition has multiple levels of parents
      // eg: For the given parent hierarchy of ["8","41","89","177"], "177" needs to be picked up
      if(!partition.parents.isEmpty) {
        val cmp = new Comparator[String]() {
          override def compare(str1: String, str2: String): Int = Integer.valueOf(str1).compareTo(Integer.valueOf(str2))
        }
        parentPartitionId = Collections.max(partition.parents, cmp)
      }

      val collectionLink = connection.getCollectionLink

      // Initialize the static tokens cache or read it from checkpoint
      def initializeToken(): Unit = {
        cfCurrentToken = CosmosDBRDDIterator.hdfsUtils.readChangeFeedTokenPartition(
          changeFeedCheckpointLocation,
          queryName,
          CosmosDBRDDIterator.getNextTokenPath(queryName),
          collectionLink,
          partitionId)

        cfNextToken = CosmosDBRDDIterator.hdfsUtils.readChangeFeedTokenPartition(
          changeFeedCheckpointLocation,
          CosmosDBRDDIterator.getNextTokenPath(queryName),
          queryName,
          collectionLink,
          partitionId)

        if(cfCurrentToken.isEmpty && !parentPartitionId.isEmpty) {
          cfCurrentToken = CosmosDBRDDIterator.hdfsUtils.readChangeFeedTokenPartition(
            changeFeedCheckpointLocation,
            queryName,
            CosmosDBRDDIterator.getNextTokenPath(queryName),
            collectionLink,
            parentPartitionId)
        }

        if(cfNextToken.isEmpty && !parentPartitionId.isEmpty) {
          cfNextToken = CosmosDBRDDIterator.hdfsUtils.readChangeFeedTokenPartition(
            changeFeedCheckpointLocation,
            CosmosDBRDDIterator.getNextTokenPath(queryName),
            queryName,
            collectionLink,
            parentPartitionId)
        }
     }

      // Get continuation token for the partition with provided partitionId
      def getContinuationToken(partitionId: String): String = {
        val continuationToken = config.get[String](CosmosDBConfig.ChangeFeedContinuationToken)
        if (continuationToken.isDefined) {
          // Continuation token is overridden
          val emptyTokenMap = new ConcurrentHashMap[String, String]()
          val collectionTokenMap = objectMapper.readValue(continuationToken.get, emptyTokenMap.getClass)
          cfCurrentToken = collectionTokenMap.get(partitionId)

          // If the new partition does not have the checkpoint file yet, get the ContinuationToken from its parents' checkpoint file
          if ((cfCurrentToken == null || cfCurrentToken.isEmpty()) && parentPartitionId != null && !parentPartitionId.isEmpty()) {
            cfCurrentToken = collectionTokenMap.get(parentPartitionId)
            logInfo(s"Null ContinuationToken for PartitionId: $partitionId. Latest Parent-PartitionId: $parentPartitionId. + " +
              s"CurrentToken for latest Parent-PartitionId: $cfCurrentToken")
          }
        } else {
          // Set the current token to next token for the target collection
          val useNextToken: Boolean = config
            .get[String](CosmosDBConfig.ChangeFeedUseNextToken)
            .getOrElse(CosmosDBConfig.DefaultChangeFeedUseNextToken.toString)
            .toBoolean
          if (useNextToken) {
            cfCurrentToken = cfNextToken
          }
        }

        cfCurrentToken
      }

      // Update the tokens cache as appropriate
      def updateTokens(nextToken: String, partitionId: String): Unit = {
        val rollingChangeFeed: Boolean = config
          .get[String](CosmosDBConfig.RollingChangeFeed)
          .getOrElse(CosmosDBConfig.DefaultRollingChangeFeed.toString)
          .toBoolean

        if (cfCurrentToken == null || cfCurrentToken.isEmpty || rollingChangeFeed) {
          cfCurrentToken = nextToken
        }

        if (!changeFeedCheckpointLocation.isEmpty) {
          CosmosDBRDDIterator.hdfsUtils.writeChangeFeedTokenPartition(
            changeFeedCheckpointLocation,
            queryName,
            collectionLink,
            partitionId,
            cfCurrentToken
          )
        }

        // Always update the next continuation
        cfNextToken = nextToken
        CosmosDBRDDIterator.hdfsUtils.writeChangeFeedTokenPartition(
          changeFeedCheckpointLocation,
          CosmosDBRDDIterator.getNextTokenPath(queryName),
          collectionLink,
          partitionId,
          cfNextToken
        )
      }

      initializeToken()

      val startFromTheBeginning: Boolean = config
        .get[String](CosmosDBConfig.ChangeFeedStartFromTheBeginning)
        .getOrElse(CosmosDBConfig.DefaultChangeFeedStartFromTheBeginning.toString)
        .toBoolean

      val startFromDateTime: String = config
        .getOrElse[String](CosmosDBConfig.ChangeFeedStartFromDateTime, "")

      val currentToken: String = getContinuationToken(partitionId)

      val changeFeedOptions: ChangeFeedOptions = new ChangeFeedOptions()
      changeFeedOptions.setPartitionKeyRangeId(partition.partitionKeyRangeId.toString)
      changeFeedOptions.setStartFromBeginning(startFromTheBeginning)
      if (StringUtils.isNotBlank(startFromDateTime)) {
        val startFromDateTimeParsed = CosmosDBRDDIterator.formatterGMT.parseDateTime(startFromDateTime);
        changeFeedOptions.setStartDateTime(startFromDateTimeParsed)
      }

      if (currentToken != null && !currentToken.isEmpty) {
        changeFeedOptions.setRequestContinuation(currentToken)
      }
      changeFeedOptions.setPageSize(pageSize)

      val structuredStreaming: Boolean = config
        .get[String](CosmosDBConfig.StructuredStreaming)
        .getOrElse(CosmosDBConfig.DefaultStructuredStreaming.toString)
        .toBoolean

      val shouldInferStreamSchema: Boolean = config
        .get[String](CosmosDBConfig.InferStreamSchema)
        .getOrElse(CosmosDBConfig.DefaultInferStreamSchema.toString)
        .toBoolean

      var iteratorDocument: Iterator[Document] = Iterator()
      var isGone: Boolean = false
      var successReadChangeFeed: Boolean = false

      while(!successReadChangeFeed && retryCount < maxRetryCountOnServiceUnavailable && !isGone) {
        // Query for change feed
        try {
          iteratorDocument = connection.readChangeFeed(changeFeedOptions, structuredStreaming, shouldInferStreamSchema, (currentToken: String, nextToken: String, partitionId: String) => updateTokens(nextToken, partitionId))
          successReadChangeFeed = true
        }
        catch {
          case docEx: DocumentClientException => handleGoneException(connection, docEx)
          case ex: IllegalStateException =>
            ex.getCause match {
              case _: ServiceUnavailableException =>
                if (retryCount < maxRetryCountOnServiceUnavailable) {
                  val retryDelayInMs = rnd.nextInt(1000)
                  logWarning(s"Service Unavailable exception thrown. Going to retry. " +
                    s"Current retry count: $retryCount. Max retry count: $maxRetryCountOnServiceUnavailable " +
                    s"Retry Delay (ms): $retryDelayInMs")
                  Thread.sleep(retryDelayInMs)
                  retryCount += 1
                } else {
                  logError("Exhausted all retries on Service Unavailable exception")
                  throw ex
                }
              case _ => ex.getCause match {
                case docEx: DocumentClientException =>
                  handleGoneException(connection, docEx)
                  isGone = true
                case _ =>
                  logError(s"An IllegalStateException was thrown: ${ex.getMessage}")
              }
            }
          case ex: Throwable =>
            logError(s"UNHANDLED EXCEPTION: ${ex.getMessage}")
            throw ex
        }
      }
      iteratorDocument
    }

    taskContext.addTaskFailureListener((_: TaskContext, ex: Throwable) => {
      logError("Handling Task Failure")
      ex match {
        case dcx: DocumentClientException =>
          if (dcx.getStatusCode == StatusCodes.SERVICE_UNAVAILABLE) {
            logError("Service Unavailable")
            connection.reinitializeClient()
          }
        case _: IllegalStateException if ex.getCause != null && ex.getCause.isInstanceOf[DocumentClientException] => {
          val dcx: DocumentClientException = ex.getCause.asInstanceOf[DocumentClientException]
          logError(s"Illegal State Exception with StatusCode ${dcx.getStatusCode}")
          if (dcx.getStatusCode == StatusCodes.SERVICE_UNAVAILABLE) {
            connection.reinitializeClient()
          }
        }
        case genericThrowable: Throwable => logError(s"Unspecific error ${genericThrowable.getMessage}")
      }
    })

    // Register an on-task-completion callback to close the input stream.
    val taskCompletionListerner = new TaskCompletionListener() {
      override def onTaskCompletion(context: TaskContext): Unit = {
        closeIfNeeded()
      }
    }

    taskContext.addTaskCompletionListener(taskCompletionListerner)

    if (!readingChangeFeed) {
      queryDocuments
    } else {
      readChangeFeed
    }
  }

  private def handleGoneException(connection: CosmosDBConnection, exception: DocumentClientException): Unit = {
    if (exception.getStatusCode == StatusCodes.SERVICE_UNAVAILABLE
      || exception.getSubStatusCode == SubStatusCodes.PARTITION_KEY_RANGE_GONE
      || exception.getSubStatusCode == SubStatusCodes.COMPLETING_SPLIT)
    {
      val retryDelayInMs = rnd.nextInt(1000)

      logWarning(
        s"STREAMING EXCEPTION: Partition ${partition.partitionKeyRangeId} is splitting, " +
        s"status code ${exception.getStatusCode}, subStatus ${exception.getSubStatusCode} " +
        s"Retry delay (ms): $retryDelayInMs")
      Thread.sleep(retryDelayInMs)
      connection.reinitializeClient()
    } else {
      logWarning(s"UNHANDLED STREAMING EXCEPTION: ${exception.getMessage} " +
        s"${exception.getStatusCode} ${exception.getSubStatusCode}")
      throw exception
    }
  }

  override def hasNext: Boolean = {
    if (maxItems != null && maxItems.isDefined && maxItems.get <= itemCount) {
      return false
    }
    !closed && reader.hasNext
  }

  override def next(): Document = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    itemCount = itemCount + 1
    reader.next()
  }

  def closeIfNeeded(): Unit = {
    if (!closed) {
      closed = true
      close()
    }
  }

  protected def close(): Unit = {
    if (initialized) {
      initialized = false
    }
  }
}
