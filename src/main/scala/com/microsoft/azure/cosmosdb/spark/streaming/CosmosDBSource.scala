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
package com.microsoft.azure.cosmosdb.spark.streaming

import com.microsoft.azure.cosmosdb.spark.LoggingTrait
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.rdd.CosmosDBRDDIterator
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

private[spark] class CosmosDBSource(sqlContext: SQLContext,
                                    configMap: Map[String, String])
  extends Source with LoggingTrait {

  val streamConfigMap: Map[String, String] = configMap.
    -(CosmosDBConfig.ReadChangeFeed).
    +((CosmosDBConfig.ReadChangeFeed, String.valueOf(true))).
    -(CosmosDBConfig.RollingChangeFeed).
    +((CosmosDBConfig.RollingChangeFeed, String.valueOf(false))).
    -(CosmosDBConfig.StructuredStreaming).
    +((CosmosDBConfig.StructuredStreaming, String.valueOf(true)))

  var currentSchema: StructType = _

  override def schema: StructType = {
    def cosmosDbStreamSchema: StructType = {
      StructType(
        Seq(
          StructField("body", StringType),
          StructField("id", StringType),
          StructField("_rid", StringType),
          StructField("_self", StringType),
          StructField("_etag", StringType),
          StructField("_attachments", StringType),
          StructField("_ts", StringType)
        ))
    }

    if (currentSchema == null) {
      CosmosDBRDDIterator.initializeHdfsUtils(HdfsUtils.getConfigurationMap(
        sqlContext.sparkSession.sparkContext.hadoopConfiguration).toMap)

      logDebug(s"Reading data to derive the schema")
      val helperDfConfig: Map[String, String] = streamConfigMap
        .-(CosmosDBConfig.ChangeFeedStartFromTheBeginning)
        .+((CosmosDBConfig.ChangeFeedStartFromTheBeginning, String.valueOf(false)))
        .-(CosmosDBConfig.ReadChangeFeed).
        +((CosmosDBConfig.ReadChangeFeed, String.valueOf(false)))
        .-(CosmosDBConfig.QueryCustom).
        +((CosmosDBConfig.QueryCustom, "SELECT TOP 10 * FROM c"))
      val shouldInferSchema = helperDfConfig.
        getOrElse(CosmosDBConfig.InferStreamSchema, CosmosDBConfig.DefaultInferStreamSchema.toString).
        toBoolean

      if (shouldInferSchema) {
        // Dummy batch read query to sample schema
        val df = sqlContext.read.cosmosDB(Config(helperDfConfig))
        val tokens = CosmosDBRDDIterator.getCollectionTokens(Config(configMap))
        if (StringUtils.isEmpty(tokens)) {
          // Empty tokens means it is a new streaming query
          // Trigger the count to force batch read query to sample schema
          df.count()
        }

        currentSchema = df.schema
      } else {
        currentSchema = cosmosDbStreamSchema
      }
    }
    currentSchema
  }

  override def getOffset: Option[Offset] = {
    var nextTokens = CosmosDBRDDIterator.getCollectionTokens(Config(streamConfigMap))
    var offset = CosmosDBOffset(nextTokens)
    logDebug(s"getOffset: $offset")
    Some(offset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    def getOffsetJsonForProgress(offsetJson: String): String = {
      val tsTokenRegex = "\"" + CosmosDBConfig.StreamingTimestampToken + "\"\\:\"[\\d]+\"" // "tsToken": "2324343"
      offsetJson.replaceAll(tsTokenRegex, StringUtils.EMPTY)
    }

    logDebug(s"getBatch with offset: $start $end")
    val endJson: String = getOffsetJsonForProgress(end.json)
    val nextTokens = getOffsetJsonForProgress(CosmosDBRDDIterator.getCollectionTokens(Config(streamConfigMap)))
    val currentTokens = getOffsetJsonForProgress(
      CosmosDBRDDIterator.getCollectionTokens(Config(streamConfigMap),
      shouldGetCurrentToken = true))

    // Only getting the data in the following cases:
    // - The provided end offset is the current offset (next tokens), the stream is progressing to the batch
    // - The provided end offset is the current tokens. This means the stream didn't get to commit the to end offset yet
    // in the previous batch. It could be due to node failures or processing failures.
    if (endJson.equals(nextTokens) || endJson.equals(currentTokens)) {
      logDebug(s"Getting data for end offset")
      val readConfig = Config(
        streamConfigMap
          .-(CosmosDBConfig.ChangeFeedContinuationToken)
          .+((CosmosDBConfig.ChangeFeedContinuationToken, end.json)))
      val currentDf = sqlContext.read.cosmosDB(schema, readConfig, sqlContext)
      currentDf
    } else {
      logDebug(s"Skipping this batch")
      sqlContext.createDataFrame(sqlContext.emptyDataFrame.rdd, schema)
    }
  }

  override def commit(end: Offset): Unit = {
    logDebug(s"Committed offset: $end")
    // no op
  }

  override def stop(): Unit = {
    // no op
  }
}
