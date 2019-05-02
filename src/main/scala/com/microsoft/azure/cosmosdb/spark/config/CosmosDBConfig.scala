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

  // Environment variable name
  val EndpointEnvVarName = "COSMOS_ENDPOINT"
  val KeyEnvVarname = "COSMOS_KEY"

  // Cosmos DB account related config
  val Endpoint = "endpoint"
  val Database = "database"
  val Collection = "collection"
  val Masterkey = "masterkey"

  val PreferredRegionsList = "preferredregions"
  val ConsistencyLevel = "consistencylevel"

  // Spark dataframe schema related config
  val SamplingRatio = "schema_samplingratio"
  val SampleSize = "schema_samplesize"

  // Connection related config
  val ConnectionMode = "connectionmode"
  val ConnectionMaxPoolSize = "connectionmaxpoolsize"
  val ConnectionIdleTimeout = "connectionidletimeout"
  val ConnectionRequestTimeout = "connectionrequesttimeout" // in seconds

  // Query related config
  val QueryCustom = "query_custom"
  val QueryPageSize = "query_pagesize"
  val QueryMaxRetryOnThrottled = "query_maxretryattemptsonthrottledrequests"
  val QueryMaxRetryWaitTimeSecs = "query_maxretrywaittimeinseconds"
  val QueryMaxDegreeOfParallelism = "query_maxdegreeofparallelism"
  val QueryMaxBufferedItemCount =  "query_maxbuffereditemcount"
  val QueryEnableScan = "query_enablescan"
  val QueryDisableRUPerMinuteUsage = "query_disableruperminuteusage"
  val QueryEmitVerboseTraces = "query_emitverbosetraces"
  val ResponseContinuationTokenLimitInKb = "response_continuationtoken_limit_kb"

  // Change feed streaming related
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
  val InferStreamSchema = "inferstreamschema"

  // Not a config, constant
  val StreamingTimestampToken = "tsToken"

  // Write path related config
  val Upsert = "upsert"
  val ClientInitDelay = "clientinitdelay"
  val RootPropertyToSave = "rootpropertytosave"

  // Bulk executor library related
  val BulkImport = "bulkimport"
  val WritingBatchSize = "writingbatchsize"
  val BulkUpdate = "bulkupdate"
  val MaxMiniBatchUpdateCount = "maxminibatchupdatecount"
  val PartitionKeyDefinition = "partitionkeydefinition"
  val WriteThroughputBudget = "writethroughputbudget"
  val BulkImportMaxConcurrencyPerPartitionRange = "bulkimport_maxconcurrencyperpartitionrange"

  // Rx Java related write config
  val WritingBatchDelayMs = "writingbatchdelayms"

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

  val ApplicationName = "application_name"

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

  val DefaultConnectionMode: String = com.microsoft.azure.documentdb.ConnectionMode.DirectHttps.toString // for sync SDK
  val DefaultConsistencyLevel: String = com.microsoft.azure.documentdb.ConsistencyLevel.Eventual.toString
  val DefaultQueryMaxRetryOnThrottled = 1000
  val DefaultQueryMaxRetryWaitTimeSecs = 1000
  val DefaultSamplingRatio = 1.0
  val DefaultPageSize = 1000
  val DefaultSampleSize = DefaultPageSize
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
  val DefaultResponseContinuationTokenLimitInKb = 10
  val DefaultWritingBatchSize_BulkInsert = 100000
  val DefaultWritingBatchSize_PointInsert = 500
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

  val DefaultInferStreamSchema = true

  val DefaultMaxConnectionPoolSize = 500

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
