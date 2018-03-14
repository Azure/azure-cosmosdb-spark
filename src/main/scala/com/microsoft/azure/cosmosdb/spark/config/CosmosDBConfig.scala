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
  val QueryMaxRetryOnThrottled = "query_maxretryattemptsonthrottledrequests"
  val QueryMaxRetryWaitTimeSecs = "query_maxretrywaittimeinseconds"
  val QueryMaxDegreeOfParallelism = "query_maxdegreeofparallelism"
  val QueryMaxBufferedItemCount =  "query_maxbuffereditemcount"
  val QueryEnableScan = "query_enablescan"
  val QueryDisableRUPerMinuteUsage = "query_disableruperminuteusage"
  val QueryEmitVerboseTraces = "query_emitverbosetraces"
  val QueryCustom = "query_custom"
  val PreferredRegionsList = "preferredregions"
  val Upsert = "upsert"
  val ConnectionMode = "connectionmode"
  val ConnectionMaxPoolSize = "connectionmaxpoolsize"
  val ConnectionIdleTimeout = "connectionidletimeout"
  val ConsistencyLevel = "consistencylevel"
  val ReadChangeFeed = "readchangefeed"
  val RollingChangeFeed = "rollingchangefeed"
  val ChangeFeedStartFromTheBeginning = "changefeedstartfromthebeginning"
  val ChangeFeedUseNextToken = "changefeedusenexttoken"
  val ChangeFeedContinuationToken = "changefeedcontinuationtoken"
  val IncrementalView = "incrementalview"
  val StructuredStreaming = "structuredstreaming"
  val CachingModeParam = "cachingmode"
  val ChangeFeedQueryName = "changefeedqueryname"
  val ChangeFeedNewQuery = "changefeednewquery"
  val ChangeFeedCheckpointLocation = "changefeedcheckpointlocation"
  val WritingBatchSize = "writingbatchsize"
  val WritingBatchDelayMs = "writingbatchdelayms"
  val StreamingTimestampToken = "tsToken"
  val RootPropertyToSave = "rootpropertytosave"
  val BulkImport = "bulkimport"
  val BulkUpdate = "bulkupdate"
  val MaxMiniBatchUpdateCount = "maxminibatchupdatecount"
  val ClientInitDelay = "clientinitdelay"
<<<<<<< HEAD
  val ParitionKeyDefinition = "partitionkeydefinition"
=======
  val PartitionKeyDefinition = "partitionkeydefinition"
>>>>>>> b4cc515513ce26195d6f7d5748bc96ad87bbf56f

  // Writing progress tracking
  val WritingBatchId = "writingbatchid"
  val CosmosDBFileStoreCollection = "cosmosdbfilestorecollection"

  // ADL import
  val adlAccountFqdn = "adlaccountfqdn"
  val adlClientId = "adlclientid"
  val adlAuthTokenEndpoint = "adlauthtokenendpoint"
  val adlClientKey = "adlclientkey"
  val adlDataFolder = "adldatafolder"
  val adlIdField = "adlidfield"
  val adlPkField = "adlpkfield"
  val adlUseGuidForId = "adluseguidforid"
  val adlUseGuidForPk = "adluseguidforpk"
  val adlFileCheckpointPath = "adlfilecheckpointpath"
  val adlCosmosDbDataCollectionPkValue = "adlcosmosdbdatacolletionpkvalue"
  val adlMaxFileCount = "adlmaxfilecount"

  // When the streaming source is slow, there will be times when getting data from a specific continuation token
  // returns no results and therefore no information on the next continuation token set is available.
  // In those cases, the connector gives a delay and then trigger the next batch.
  val StreamingSlowSourceDelayMs = "streamingslowsourcedelayms"

  // Mandatory
  val required = List(
    Endpoint,
    Masterkey,
    Database,
    Collection
  )

  val DefaultSamplingRatio = 1.0
  val DefaultPageSize = 1000
  val DefaultSampleSize = DefaultPageSize
  val DefaultConnectionMode: String = com.microsoft.azure.documentdb.ConnectionMode.DirectHttps.toString
  val DefaultConsistencyLevel: String = com.microsoft.azure.documentdb.ConsistencyLevel.Session.toString
  val DefaultUpsert = false
  val DefaultReadChangeFeed = false
  val DefaultStructuredStreaming = false
  val DefaultRollingChangeFeed = false
  val DefaultChangeFeedStartFromTheBeginning = false
  val DefaultChangeFeedUseNextToken = false
  val DefaultIncrementalView = false
  val DefaultCacheMode = CachingMode.NONE
  val DefaultChangeFeedNewQuery = false
  val DefaultQueryMaxDegreeOfParallelism = Integer.MAX_VALUE
  val DefaultQueryMaxBufferedItemCount = Integer.MAX_VALUE
  val DefaultWritingBatchSize = 500
  val DefaultWritingBatchDelayMs = 0
  val DefaultStreamingSlowSourceDelayMs = 1
  val DefaultBulkImport = true
  val DefaultBulkUpdate = false
  val DefaultMaxMiniBatchUpdateCount = 500
  val DefaultClientInitDelay = 10

  val DefaultAdlUseGuidForId = true
  val DefaultAdlUseGuidForPk = true
  val DefaultAdlMaxFileCount: Int = Int.MaxValue

  val SinglePartitionCollectionOfferThroughput = 10000

  def parseParameters(parameters: Map[String, String]): Map[String, Any] = {
    return parameters.map { case (x, v) => x -> v }
  }
}

/**
  * Represents caching behavior, used internally in incremental view mode.
  *
  * None: data is not cached
  * Cache: data is cached by invoking rdd.cache()
  * RefreshCache: discard current cache data and read the latest data into cache
  */
object CachingMode extends Enumeration {
  type CachingMode = Value
  val NONE = Value("None")
  val CACHE = Value("Cache")
  val REFRESH_CACHE = Value("RefreshCache")
}
