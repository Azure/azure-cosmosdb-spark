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

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.partitioner.CosmosDBPartition
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.{CosmosDBConnection, LoggingTrait}
import com.microsoft.azure.documentdb._
import org.apache.commons.lang3.StringUtils
import org.apache.spark._
import org.apache.spark.sql.sources.Filter

object CosmosDBRDDIterator {

  // For verification purpose
  var lastFeedOptions: FeedOptions = _

  // Map of change feed query name -> collection Rid -> partition range ID -> continuation token
  var changeFeedTokens: ConcurrentMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]] = _
  // Map of change feed candidate token for the next batch
  var changeFeedNextTokens: ConcurrentMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]] = _
}

class CosmosDBRDDIterator(
                             taskContext: TaskContext,
                             partition: CosmosDBPartition,
                             config: Config,
                             maxItems: Option[Long],
                             requiredColumns: Array[String],
                             filters: Array[Filter])
  extends Iterator[Document]
    with LoggingTrait {

  private var closed = false
  private var initialized = false
  private var itemCount: Long = 0

  lazy val reader: Iterator[Document] = {
    initialized = true
    var connection: CosmosDBConnection = new CosmosDBConnection(config)

    val readingChangeFeed: Boolean = config
      .get[String](CosmosDBConfig.ReadChangeFeed)
      .getOrElse(CosmosDBConfig.DefaultReadChangeFeed.toString)
      .toBoolean

    def queryDocuments: Iterator[Document] = {
      val feedOpts = new FeedOptions()
      val pageSize: Int = config
        .get[String](CosmosDBConfig.QueryPageSize)
        .getOrElse(CosmosDBConfig.DefaultPageSize.toString)
        .toInt
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
      feedOpts.setPartitionKeyRangeIdInternal(partition.partitionKeyRangeId.toString)
      CosmosDBRDDIterator.lastFeedOptions = feedOpts

      val queryString = config
        .get[String](CosmosDBConfig.QueryCustom)
        .getOrElse(FilterConverter.createQueryString(requiredColumns, filters))
      logDebug(s"CosmosDBRDDIterator::LazyReader, convert to predicate: $queryString")

      connection.queryDocuments(queryString, feedOpts)
    }

    def readChangeFeed: Iterator[Document] = {

      // For tokens checkpointing
      var changeFeedCheckpoint: Boolean = false
      var checkPointPath: String = null
      val objectMapper: ObjectMapper = new ObjectMapper()

      var collectionTokenMap: ConcurrentMap[String, String] = null
      var collectionNextTokenMap: ConcurrentMap[String, String] = null

      def initializeTokenMaps() = {
        if (CosmosDBRDDIterator.changeFeedTokens == null) {
          CosmosDBRDDIterator.synchronized {
            if (CosmosDBRDDIterator.changeFeedTokens == null) {
              val emptyChangeFeedContinuationTokens =
                new ConcurrentHashMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]]

              // Read continuation tokens from checkpoint location
              val changeFeedCheckpointLocation: String = config
                .get[String](CosmosDBConfig.ChangeFeedCheckpointLocation)
                .getOrElse(StringUtils.EMPTY)
              if (!StringUtils.isEmpty(changeFeedCheckpointLocation)) {
                changeFeedCheckpoint = true
                checkPointPath = Paths.get(changeFeedCheckpointLocation, "changeFeedCheckPoint").toString
                val checkPointFile = new File(checkPointPath)
                if (checkPointFile.exists()) {
                  CosmosDBRDDIterator.changeFeedTokens =
                    objectMapper.readValue(checkPointFile, emptyChangeFeedContinuationTokens.getClass)
                  logInfo(s"Read change feed continuation tokens from $checkPointPath")
                } else {
                  logInfo(s"Using new change feed checkpoint file $checkPointPath")
                }
              }

              if (CosmosDBRDDIterator.changeFeedTokens == null) {
                CosmosDBRDDIterator.changeFeedTokens =
                  new ConcurrentHashMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]]
                CosmosDBRDDIterator.changeFeedNextTokens =
                  new ConcurrentHashMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]]
              }
            }
          }
        }
      }

      def getContinuationToken(partitionId: String): String = {
        val collectionLink = connection.collectionLink
        val queryName = config
          .get[String](CosmosDBConfig.ChangeFeedQueryName).get

        CosmosDBRDDIterator.changeFeedTokens.putIfAbsent(
          queryName,
          new ConcurrentHashMap[String, ConcurrentMap[String, String]]())
        CosmosDBRDDIterator.changeFeedNextTokens.putIfAbsent(
          queryName,
          new ConcurrentHashMap[String, ConcurrentMap[String, String]]())

        val currentTokens = CosmosDBRDDIterator.changeFeedTokens.get(queryName)
        val nextTokens = CosmosDBRDDIterator.changeFeedNextTokens.get(queryName)

        currentTokens.putIfAbsent(collectionLink, new ConcurrentHashMap[String, String]())
        nextTokens.putIfAbsent(collectionLink, new ConcurrentHashMap[String, String]())
        collectionTokenMap = currentTokens.get(collectionLink)
        collectionNextTokenMap = nextTokens.get(collectionLink)

        // Set the current token to next token for the target collection
        val useNextToken: Boolean = config
          .get[String](CosmosDBConfig.ChangeFeedUseNextToken)
          .getOrElse(CosmosDBConfig.DefaultChangeFeedUseNextToken.toString)
          .toBoolean
        if (useNextToken) {
          val nextToken = collectionNextTokenMap.get(partitionId)
          if (nextToken != null) {
            collectionTokenMap.put(partitionId, nextToken)
          }
        }

        val changeFeedContinuation = collectionTokenMap.get(partitionId)
        changeFeedContinuation
      }

      def updateTokens(currentToken: String,
                       nextToken: String,
                       partitionId: String,
                       collectionTokenMap: ConcurrentMap[String, String],
                       collectionNextTokenMap: ConcurrentMap[String, String]) = {
        val rollingChangeFeed: Boolean = config
          .get[String](CosmosDBConfig.RollingChangeFeed)
          .getOrElse(CosmosDBConfig.DefaultRollingChangeFeed.toString)
          .toBoolean

        if (currentToken == null || rollingChangeFeed) {
          collectionTokenMap.put(partitionId, nextToken)

          if (changeFeedCheckpoint) {
            CosmosDBRDDIterator.synchronized {
              objectMapper.writeValue(new File(checkPointPath), CosmosDBRDDIterator.changeFeedTokens)
            }
          }
        }
        // Always update the next continuation token map
        collectionNextTokenMap.put(partitionId, nextToken)
      }

      initializeTokenMaps()

      val partitionId = partition.partitionKeyRangeId.toString
      val startFromTheBeginning: Boolean = config
        .get[String](CosmosDBConfig.ChangeFeedStartFromTheBeginning)
        .getOrElse(CosmosDBConfig.DefaultChangeFeedStartFromTheBeginning.toString)
        .toBoolean
      val currentToken: String = getContinuationToken(partitionId)

      val changeFeedOptions: ChangeFeedOptions = new ChangeFeedOptions()
      changeFeedOptions.setPartitionKeyRangeId(partition.partitionKeyRangeId.toString)
      changeFeedOptions.setStartFromBeginning(startFromTheBeginning)
      if (currentToken != null) {
        changeFeedOptions.setRequestContinuation(currentToken)
      }

      // Query for change feed
      val response = connection.readChangeFeed(changeFeedOptions)
      val iteratorDocument = response._1
      val nextToken = response._2

      updateTokens(currentToken, nextToken, partitionId, collectionTokenMap, collectionNextTokenMap)

      logDebug(s"changeFeedOptions.partitionKeyRangeId = ${changeFeedOptions.getPartitionKeyRangeId}, continuation = $currentToken, new token = ${response._2}, iterator.hasNext = ${response._1.hasNext}")

      iteratorDocument
    }

    if (!readingChangeFeed) {
      queryDocuments
    } else {
      readChangeFeed
    }
  }

  // Register an on-task-completion callback to close the input stream.
  taskContext.addTaskCompletionListener((context: TaskContext) => closeIfNeeded())

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
