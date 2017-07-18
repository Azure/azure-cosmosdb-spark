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
  var changeFeedContinuationTokens: ConcurrentMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]] = _

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
    var conn: CosmosDBConnection = new CosmosDBConnection(config)

    val readingChangeFeed: Boolean = config
      .get[String](CosmosDBConfig.ReadChangeFeed)
      .getOrElse(CosmosDBConfig.DefaultReadChangeFeed.toString)
      .toBoolean
    val rollingChangeFeed: Boolean = config
      .get[String](CosmosDBConfig.RollingChangeFeed)
      .getOrElse(CosmosDBConfig.DefaultRollingChangeFeed.toString)
      .toBoolean

    if (!readingChangeFeed) {
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
      // Set target partition ID_PROPERTY
      feedOpts.setPartitionKeyRangeIdInternal(partition.partitionKeyRangeId.toString)
      feedOpts.setEnableCrossPartitionQuery(true)
      CosmosDBRDDIterator.lastFeedOptions = feedOpts

      val queryString = config
        .get[String](CosmosDBConfig.QueryCustom)
        .getOrElse(FilterConverter.createQueryString(requiredColumns, filters))
      logDebug(s"CosmosDBRDDIterator::LazyReader, convert to predicate: $queryString")

      conn.queryDocuments(queryString, feedOpts)
    } else {
      // Initialize change feed continuation tokens
      var changeFeedCheckpoint: Boolean = false
      var checkPointPath: String = null
      val objectMapper: ObjectMapper = new ObjectMapper()

      if (CosmosDBRDDIterator.changeFeedContinuationTokens == null) {

        CosmosDBRDDIterator.synchronized {

          if (CosmosDBRDDIterator.changeFeedContinuationTokens == null) {

            val changeFeedCheckpointLocation: String = config
              .get[String](CosmosDBConfig.ChangeFeedCheckpointLocation)
              .getOrElse(StringUtils.EMPTY)
            val emptyChangeFeedContinuationTokens =
              new ConcurrentHashMap[String, ConcurrentMap[String, ConcurrentMap[String, String]]]

            if (!StringUtils.isEmpty(changeFeedCheckpointLocation)) {
              changeFeedCheckpoint = true
              checkPointPath = Paths.get(changeFeedCheckpointLocation, "changeFeedCheckPoint").toString
              val checkPointFile = new File(checkPointPath)
              if (checkPointFile.exists()) {
                CosmosDBRDDIterator.changeFeedContinuationTokens =
                  objectMapper.readValue(checkPointFile, emptyChangeFeedContinuationTokens.getClass)
                logInfo(s"Read change feed continuation tokens from $checkPointPath")
              } else {
                logInfo(s"Using new change feed checkpoint file $checkPointPath")
              }
            }
            if (CosmosDBRDDIterator.changeFeedContinuationTokens == null) {
              CosmosDBRDDIterator.changeFeedContinuationTokens = emptyChangeFeedContinuationTokens
            }
          }
        }
      }

      val changeFeedQueryName = config
        .get[String](CosmosDBConfig.ChangeFeedQueryName).get
      CosmosDBRDDIterator.changeFeedContinuationTokens.putIfAbsent(changeFeedQueryName,
        new ConcurrentHashMap[String, ConcurrentMap[String, String]]())
      val currentContinuationTokens: ConcurrentMap[String, ConcurrentMap[String, String]] =
        CosmosDBRDDIterator.changeFeedContinuationTokens.get(changeFeedQueryName)

      val changeFeedOptions: ChangeFeedOptions = new ChangeFeedOptions()
      changeFeedOptions.setPartitionKeyRangeId(partition.partitionKeyRangeId.toString)

      val collectionLink = conn.collectionLink
      currentContinuationTokens.putIfAbsent(collectionLink, new ConcurrentHashMap[String, String]())

      val collectionContinuationMap = currentContinuationTokens.get(collectionLink)
      val changeFeedContinuation = collectionContinuationMap.get(partition.partitionKeyRangeId.toString)
      if (changeFeedContinuation != null) {
        changeFeedOptions.setRequestContinuation(changeFeedContinuation)
      }

      val response = conn.readChangeFeed(changeFeedOptions)

      if (changeFeedContinuation == null || rollingChangeFeed) {
        collectionContinuationMap.put(partition.partitionKeyRangeId.toString, response._2)

        if (changeFeedCheckpoint) {
          CosmosDBRDDIterator.synchronized {
            objectMapper.writeValue(new File(checkPointPath), CosmosDBRDDIterator.changeFeedContinuationTokens)
          }
        }
      }

      logInfo(s"changeFeedOptions.partitionKeyRangeId = ${changeFeedOptions.getPartitionKeyRangeId}, continuation = $changeFeedContinuation, new token = ${response._2}, iterator.hasNext = ${response._1.hasNext}")

      response._1
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
