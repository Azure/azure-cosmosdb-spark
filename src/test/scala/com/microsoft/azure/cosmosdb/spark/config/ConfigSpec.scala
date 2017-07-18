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
      val readConfig = Config(Map("Endpoint" -> CosmosDBDefaults().EMULATOR_ENDPOINT,
        "Masterkey" -> CosmosDBDefaults().EMULATOR_MASTERKEY,
        "Database" -> CosmosDBDefaults().DATABASE_NAME,
        "Collection" -> collectionName))

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
    }

  it should "be able to override the defaults" in withSparkSession() { ss =>
    val readConfig = Config(Map("Endpoint" -> CosmosDBDefaults().EMULATOR_ENDPOINT,
      "Masterkey" -> CosmosDBDefaults().EMULATOR_MASTERKEY,
      "Database" -> CosmosDBDefaults().DATABASE_NAME,
      "Collection" -> collectionName,
      "ConsistencyLevel" -> "Strong",
      "conNeCtIoNMODE" -> "Gateway",
      "upsert" -> "true",
      "schema_samplingratio" -> "0.5",
      "schema_samplesize" -> "200",
      "query_pagesize" -> "300",
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

    CosmosDBConnection.lastConsistencyLevel.get.toString should equal(readConfig.properties("consistencylevel").toString)

    val connectionPolicy = CosmosDBConnection.lastConnectionPolicy
    connectionPolicy.getConnectionMode.toString should equal(readConfig.properties("connectionmode").toString)
    connectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests.toString should
      equal(readConfig.properties("query_maxretryattemptsonthrottledrequests"))
    connectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds.toString should
      equal(readConfig.properties("query_maxretrywaittimeinseconds"))
    connectionPolicy.getPreferredLocations.size() should equal(2)

    val feedOptions = CosmosDBRDDIterator.lastFeedOptions
    feedOptions.getPageSize.toString should equal(readConfig.properties("query_pagesize"))
    feedOptions.getMaxDegreeOfParallelism.toString should equal(readConfig.properties("query_maxdegreeofparallelism"))
    feedOptions.getMaxBufferedItemCount.toString should equal(readConfig.properties("query_maxbuffereditemcount"))
    feedOptions.getEnableScanInQuery.toString should equal(readConfig.properties("query_enablescan"))
    feedOptions.getDisableRUPerMinuteUsage.toString should equal(readConfig.properties("query_disableruperminuteusage"))
    feedOptions.getEmitVerboseTracesInQuery.toString should equal(readConfig.properties("query_emitverbosetraces"))

    CosmosDBRelation.lastSamplingRatio.toString should equal(readConfig.properties("schema_samplingratio"))
    CosmosDBRelation.lastSampleSize.toString should equal(readConfig.properties("schema_samplesize"))

    CosmosDBSpark.lastUpsertSetting.get.toString should equal(readConfig.properties("upsert"))
  }
}
