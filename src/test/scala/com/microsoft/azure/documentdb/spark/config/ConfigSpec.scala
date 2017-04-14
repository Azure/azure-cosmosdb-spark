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
package com.microsoft.azure.documentdb.spark.config

import com.microsoft.azure.documentdb.ConnectionPolicy
import com.microsoft.azure.documentdb.spark.rdd.DocumentDBRDDIterator
import com.microsoft.azure.documentdb.spark.schema._
import com.microsoft.azure.documentdb.spark.{DocumentDBConnection, DocumentDBDefaults, DocumentDBSpark, RequiresDocumentDB}

class ConfigSpec extends RequiresDocumentDB {
    "Config" should "have the expected defaults" in withSparkSession() { ss =>
      val readConfig = Config(Map("Endpoint" -> DocumentDBDefaults().EMULATOR_ENDPOINT,
        "Masterkey" -> DocumentDBDefaults().EMULATOR_MASTERKEY,
        "Database" -> DocumentDBDefaults().DATABASE_NAME,
        "Collection" -> collectionName))

      val df = ss.sqlContext.read.DocumentDB(readConfig)
      df.collect()
      df.write.documentDB(readConfig)

      DocumentDBConnection.lastConsistencyLevel.get.toString should equal(DocumentDBConfig.DefaultConsistencyLevel)

      val plainConnectionPolicy = new ConnectionPolicy()
      val connectionPolicy = DocumentDBConnection.lastConnectionPolicy
      connectionPolicy.getConnectionMode.toString should equal(DocumentDBConfig.DefaultConnectionMode)
      connectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests should
        equal(plainConnectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests)
      connectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds should
        equal(plainConnectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds)
      connectionPolicy.getPreferredLocations should equal(null)

      val feedOptions = DocumentDBRDDIterator.lastFeedOptions
      feedOptions.getPageSize should equal(DocumentDBConfig.DefaultPageSize)

      DocumentDBRelation.lastSamplingRatio should equal(DocumentDBConfig.DefaultSamplingRatio)
      DocumentDBRelation.lastSampleSize should equal(DocumentDBConfig.DefaultSampleSize)

      DocumentDBSpark.lastUpsertSetting.get should equal(DocumentDBConfig.DefaultUpsert)
    }

  it should "be able to override the defaults" in withSparkSession() { ss =>
    val readConfig = Config(Map("Endpoint" -> DocumentDBDefaults().EMULATOR_ENDPOINT,
      "Masterkey" -> DocumentDBDefaults().EMULATOR_MASTERKEY,
      "Database" -> DocumentDBDefaults().DATABASE_NAME,
      "Collection" -> collectionName,
      "ConsistencyLevel" -> "Strong",
      "conNeCtIoNMODE" -> "Gateway",
      "upsert" -> "true",
      "schema_samplingratio" -> "0.5",
      "schema_samplesize" -> "200",
      "query_pagesize" -> "300",
      "query_maxretryattemptsonthrottledrequests" -> "15",
      "query_maxretrywaittimeinseconds" -> "3",
      "preferredregions" -> "West US; West US 2")
    )

    val df = ss.sqlContext.read.DocumentDB(readConfig)
    df.collect()
    df.write.documentDB(readConfig)

    DocumentDBConnection.lastConsistencyLevel.get.toString should equal(readConfig.properties("consistencylevel").toString)

    val connectionPolicy = DocumentDBConnection.lastConnectionPolicy
    connectionPolicy.getConnectionMode.toString should equal(readConfig.properties("connectionmode").toString)
    connectionPolicy.getRetryOptions.getMaxRetryAttemptsOnThrottledRequests.toString should
      equal(readConfig.properties("query_maxretryattemptsonthrottledrequests"))
    connectionPolicy.getRetryOptions.getMaxRetryWaitTimeInSeconds.toString should
      equal(readConfig.properties("query_maxretrywaittimeinseconds"))
    connectionPolicy.getPreferredLocations.size() should equal(2)

    val feedOptions = DocumentDBRDDIterator.lastFeedOptions
    feedOptions.getPageSize.toString should equal(readConfig.properties("query_pagesize"))

    DocumentDBRelation.lastSamplingRatio.toString should equal(readConfig.properties("schema_samplingratio"))
    DocumentDBRelation.lastSampleSize.toString should equal(readConfig.properties("schema_samplesize"))

    DocumentDBSpark.lastUpsertSetting.get.toString should equal(readConfig.properties("upsert"))
  }
}
