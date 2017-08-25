/**
  * The MIT License (MIT)
  * Copyright (c) 2017 Microsoft Corporation
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
package com.microsoft.azure.cosmosdb.spark.config

import com.microsoft.azure.cosmosdb.spark.{CosmosDBConnection, CosmosDBDefaults, CosmosDBSpark, RequiresCosmosDB}
import com.microsoft.azure.documentdb.ConnectionPolicy
import com.microsoft.azure.cosmosdb.spark.rdd.CosmosDBRDDIterator
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRelation

class ConfigSpec extends RequiresCosmosDB {
    "Config" should "have the expected defaults" in withSparkSession() { ss =>
      val readConfig = Config(Map("Endpoint" -> CosmosDBDefaults().CosmosDBEndpoint,
        "Masterkey" -> CosmosDBDefaults().CosmosDBKey,
        "Database" -> CosmosDBDefaults().DatabaseName,
        "Collection" -> getTestCollectionName))

      val df = ss.sqlContext.read.cosmosDB(readConfig)
      df.collect()
      df.write.cosmosDB(readConfig)

      CosmosDBConnection.lastConsistencyLevel.get.toString should equal(CosmosDBConfig.DefaultConsistencyLevel)

      val plainConnectionPolicy = new ConnectionPolicy()
      val connectionPolicy = CosmosDBConnection.lastConnectionPolicy
      connectionPolicy.getConnectionMode.toString should equal(CosmosDBConfig.DefaultConnectionMode)
      connectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests should
        equal(plainConnectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests)
      connectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds should
        equal(plainConnectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds)
      connectionPolicy.getPreferredLocations should equal(null)

      val feedOptions = CosmosDBRDDIterator.lastFeedOptions
      feedOptions.getPageSize should equal(CosmosDBConfig.DefaultPageSize)

      CosmosDBRelation.lastSamplingRatio should equal(CosmosDBConfig.DefaultSamplingRatio)
      CosmosDBRelation.lastSampleSize should equal(CosmosDBConfig.DefaultSampleSize)

      CosmosDBSpark.lastUpsertSetting.get should equal(CosmosDBConfig.DefaultUpsert)
      CosmosDBSpark.lastWritingBatchSize.get should equal(CosmosDBConfig.DefaultWritingBatchSize)
    }

  it should "be able to override the defaults" in withSparkSession() { ss =>
    val readConfig = Config(Map("Endpoint" -> CosmosDBDefaults().CosmosDBEndpoint,
      "Masterkey" -> CosmosDBDefaults().CosmosDBKey,
      "Database" -> CosmosDBDefaults().DatabaseName,
      "Collection" -> getTestCollectionName,
      "ConsistencyLevel" -> "Eventual",
      "conNeCtIoNMODE" -> "Gateway",
      "upsert" -> "true",
      "schema_samplingratio" -> "0.5",
      "schema_samplesize" -> "200",
      "query_pagesize" -> "300",
      "writIngbatchSize" -> "100",
      "query_maxretryattemptsonthrottledrequests" -> "15",
      "query_maxretrywaittimeinseconds" -> "3",
      "query_maxdegreeofparallelism" -> "100",
      "query_maxbuffereditemcount" -> "500",
      "query_enablescan" -> "true",
      "query_disableruperminuteusage" -> "true",
      "query_emitverbosetraces" -> "true",
      "preferredregions" -> "West US; West US 2")
    )

    val df = ss.sqlContext.read.cosmosDB(readConfig)
    df.collect()
    df.write.cosmosDB(readConfig)

    CosmosDBConnection.lastConsistencyLevel.get.toString should
      equal(readConfig.properties(CosmosDBConfig.ConsistencyLevel).toString)

    val connectionPolicy = CosmosDBConnection.lastConnectionPolicy
    connectionPolicy.getConnectionMode.toString should equal(readConfig.properties(CosmosDBConfig.ConnectionMode).toString)
    connectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests.toString should
      equal(readConfig.properties(CosmosDBConfig.QueryMaxRetryOnThrottled))
    connectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds.toString should
      equal(readConfig.properties(CosmosDBConfig.QueryMaxRetryWaitTimeSecs))
    connectionPolicy.getPreferredLocations.size() should equal(2)

    val feedOptions = CosmosDBRDDIterator.lastFeedOptions
    feedOptions.getPageSize.toString should equal(readConfig.properties(CosmosDBConfig.QueryPageSize))
    feedOptions.getMaxDegreeOfParallelism.toString should equal(readConfig.properties(CosmosDBConfig.QueryMaxDegreeOfParallelism))
    feedOptions.getMaxBufferedItemCount.toString should equal(readConfig.properties(CosmosDBConfig.QueryMaxBufferedItemCount))
    feedOptions.getEnableScanInQuery.toString should equal(readConfig.properties(CosmosDBConfig.QueryEnableScan))
    feedOptions.getDisableRUPerMinuteUsage.toString should equal(readConfig.properties(CosmosDBConfig.QueryDisableRUPerMinuteUsage))
    feedOptions.getEmitVerboseTracesInQuery.toString should equal(readConfig.properties(CosmosDBConfig.QueryEmitVerboseTraces))

    CosmosDBRelation.lastSamplingRatio.toString should equal(readConfig.properties(CosmosDBConfig.SamplingRatio))
    CosmosDBRelation.lastSampleSize.toString should equal(readConfig.properties(CosmosDBConfig.SampleSize))

    CosmosDBSpark.lastUpsertSetting.get.toString should equal(readConfig.properties(CosmosDBConfig.Upsert))
    CosmosDBSpark.lastWritingBatchSize.get.toString should equal(readConfig.properties(CosmosDBConfig.WritingBatchSize))
  }
}
