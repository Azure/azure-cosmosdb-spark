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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

private[spark] class CosmosDBSource(sqlContext: SQLContext,
                                    configMap: Map[String, String])
  extends Source with LoggingTrait {

  val streamConfigMap: Map[String, String] = configMap.
    -(CosmosDBConfig.ReadChangeFeed).
    +((CosmosDBConfig.ReadChangeFeed, String.valueOf(true))).
    -(CosmosDBConfig.RollingChangeFeed).
    +((CosmosDBConfig.RollingChangeFeed, String.valueOf(false)))

  var currentSchema: StructType = _

  override def schema: StructType = {
    logInfo(s"CosmosDBSource.schema is called")
    // Try to read a non-empty schema
    if (currentSchema == null || currentSchema.fields.length == 0) {
      val df = sqlContext.read.cosmosDB(Config(streamConfigMap
        .-(CosmosDBConfig.ChangeFeedStartFromTheBeginning)
        .+((CosmosDBConfig.ChangeFeedStartFromTheBeginning, String.valueOf(false)))))
      CosmosDBRDDIterator.initializeHdfsUtils(HdfsUtils.getConfigurationMap(
        sqlContext.sparkSession.sparkContext.hadoopConfiguration).toMap)
      val tokens = CosmosDBRDDIterator.getCollectionTokens(Config(configMap))
      if (StringUtils.isEmpty(tokens)) {
        // Empty tokens means it is a new streaming query
        // Trigger the count to update the current continuation token
        df.count()
      }
      currentSchema = df.schema
    }
    currentSchema
  }

  override def getOffset: Option[Offset] = {
    logInfo(s"getOffset called")
    val currentTokens = CosmosDBRDDIterator.getCollectionTokens(Config(streamConfigMap))
    var offset = CosmosDBOffset(currentTokens)
    logInfo(s"getOffset offset: $offset")
    Some(offset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch with offset: $start $end")
    // Only continue if the provided end offset is the current offset
    if (end.json.equals(getOffset.get.json)) {
      val readConfig = Config(
        streamConfigMap
          .-(CosmosDBConfig.ChangeFeedContinuationToken)
          .+((CosmosDBConfig.ChangeFeedContinuationToken, end.json)))
      sqlContext.read.cosmosDB(schema, readConfig)
    } else {
      sqlContext.createDataFrame(sqlContext.emptyDataFrame.rdd, schema)
    }
  }

  override def commit(end: Offset): Unit = {
    logInfo(s"committed offset: $end")
    // no op
  }

  override def stop(): Unit = {
    // no op
  }
}
