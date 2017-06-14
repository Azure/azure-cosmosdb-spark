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
package com.microsoft.azure.cosmosdb.spark.config

/**
 * Values and Functions for access and parse the configuration parameters
 */
object CosmosDBConfig {

  //  Parameter names
  val Endpoint = "endpoint"
  val Database = "database"
  val Collection = "collection"
  val Masterkey = "masterkey"
  val SamplingRatio = "schema_samplingratio"
  val SampleSize = "schema_samplesize"
  val QueryPageSize = "query_pagesize"
  val MaxRetryOnThrottled = "query_maxretryattemptsonthrottledrequests"
  val MaxRetryWaitTimeSecs = "query_maxretrywaittimeinseconds"
  val QueryCustom = "query_custom"
  val PreferredRegionsList = "preferredregions"
  val Upsert = "upsert"
  val ConnectionMode = "connectionmode"
  val ConsistencyLevel = "consistencylevel"
  val ReadChangeFeed = "readchangefeed"
  val RollingChangeFeed = "rollingchangefeed"
  val IncrementalView = "incrementalview"
  val CachingModeParam = "cachingmode"

  // Mandatory
  val required = List(
    Endpoint,
    Masterkey,
    Database,
    Collection
  )

  val DefaultSamplingRatio = 1.0
  val DefaultPageSize = 300
  val DefaultSampleSize = DefaultPageSize
  val DefaultConnectionMode: String = com.microsoft.azure.documentdb.ConnectionMode.DirectHttps.toString
  val DefaultConsistencyLevel: String = com.microsoft.azure.documentdb.ConsistencyLevel.Session.toString
  val DefaultUpsert = false
  val DefaultReadChangeFeed = false
  val DefaultRollingChangeFeed = false
  val DefaultIncrementalView = false
  val DefaultCacheMode = 0

  def parseParameters(parameters: Map[String, String]): Map[String, Any] = {
    return parameters.map { case (x, v) => x -> v }
  }
}

object CachingMode extends Enumeration {
  type WeekDay = Value
  val NONE = Value("None")
  val CACHE = Value("Cache")
  val REFRESH_CACHE = Value("RefreshCache")
}
