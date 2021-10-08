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
import com.microsoft.azure.cosmosdb.spark.config

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
  val ResourceToken = "resourcetoken"

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
  val ConvertNestedDocsToNativeJsonFormat = "convertnesteddocstonativejsonformat"

  // Change feed streaming related
  val ReadChangeFeed = "readchangefeed"
  val RollingChangeFeed = "rollingchangefeed"
  val ChangeFeedStartFromTheBeginning = "changefeedstartfromthebeginning"
  val ChangeFeedStartFromDateTime = "changefeedstartfromdatetime"
  val ChangeFeedUseNextToken = "changefeedusenexttoken"
  val ChangeFeedContinuationToken = "changefeedcontinuationtoken"
  val ChangeFeedMaxPagesPerBatch = "changefeedmaxpagesperbatch"
  val IncrementalView = "incrementalview"
  val StructuredStreaming = "structuredstreaming"
  val CachingModeParam = "cachingmode"
  val ChangeFeedQueryName = "changefeedqueryname"
    val ChangeFeedCheckpointLocation = "changefeedcheckpointlocation"
  val InferStreamSchema = "inferstreamschema"

  // Structured Streaming WriteStream retry policy related
  val WriteStreamRetryPolicyKind = "writestreamretrypolicy.kind"
  val MaxTransientRetryCount = "writestreamretrypolicy.maxtransientretrycount"
  val MaxTransientRetryDurationInMs = "writestreamretrypolicy.maxtransientretrydurationinms"
  val MaxTransientRetryDelayInMs = "writestreamretrypolicy.maxtransientretrydelayinms"
  val PoisonMessageLocation = "writestreamretrypolicy.poisonmessagelocation"
  val TreatUnknownExceptionsAsTransient = "writestreamretrypolicy.treatunknownexceptionsastransient"
  val DefaultWriteStreamRetryPolicyKind = "NoRetries"
  val DefaultMaxTransientRetryCount: Int = Int.MaxValue
  val DefaultMaxTransientRetryDurationInMs: Int = 1000 * 60 * 60 // 1 hour
  val DefaultMaxTransientRetryDelayInMs = 100 // 0.1 second
  val DefaultPoisonMessageLocation = ""
  val DefaultTreatUnknownExceptionsAsTransient = true

  val QueryLoggingPath = "queryLoggingPath"
  val QueryLoggingCorrelationId = "queryLoggingCorrelationId"
  val CountLoggingPath = "countLoggingPath"

  // Not a config, constant
  val StreamingTimestampToken = "tsToken"

  // Write path related config
  val Upsert = "upsert"
  val RootPropertyToSave = "rootpropertytosave"
  val PreserveNullInWrite = "preservenullinwrite"

  // Bulk executor library related
  val BulkImport = "bulkimport"
  val WritingBatchSize = "writingbatchsize"
  val BulkUpdate = "bulkupdate"
  val MaxMiniBatchUpdateCount = "maxminibatchupdatecount"
  val PartitionKeyDefinition = "partitionkeydefinition"
  val WriteThroughputBudget = "writethroughputbudget"
  val BulkImportMaxConcurrencyPerPartitionRange = "bulkimport_maxconcurrencyperpartitionrange"
  val MaxMiniBatchImportSizeKB = "maxminibatchimportsizekb"
  val BaseMiniBatchRUConsumption = "baseminibatchruconsumption"
  val MaxIngestionTaskParallelism = "maxingestiontaskparallelism"

  // Rx Java related write config
  val WritingBatchDelayMs = "writingbatchdelayms"

  val ApplicationName = "application_name"

  // When the streaming source is slow, there will be times when getting data from a specific continuation token
  // returns no results and therefore no information on the next continuation token set is available.
  // In those cases, the connector gives a delay and then trigger the next batch.
  val StreamingSlowSourceDelayMs = "streamingslowsourcedelayms"

  // Mandatory
  val required = List(
    Endpoint,
    Database,
    Collection
  )

  val DefaultConnectionMode: String = com.microsoft.azure.documentdb.ConnectionMode.DirectHttps.toString // for sync SDK
  val DefaultConsistencyLevel: String = com.microsoft.azure.documentdb.ConsistencyLevel.Eventual.toString
  val DefaultQueryMaxRetryOnThrottled = 1000
  val DefaultQueryMaxRetryWaitTimeSecs = 1000
  val DefaultSamplingRatio = 1.0
  val DefaultPageSize = 1000
  val DefaultSampleSize: Int = DefaultPageSize
  val DefaultUpsert = false
  val DefaultPreserveNullInWrite = false
  val DefaultReadChangeFeed = false
  val DefaultStructuredStreaming = false
  val DefaultRollingChangeFeed = false
  val DefaultChangeFeedStartFromTheBeginning = false
  val DefaultChangeFeedUseNextToken = false
  val DefaultChangeFeedMaxPagesPerBatch: Int = Integer.MAX_VALUE
  val DefaultIncrementalView = false
  val DefaultCacheMode: CachingMode.Value = CachingMode.NONE
  val DefaultQueryMaxDegreeOfParallelism: Int = Integer.MAX_VALUE
  val DefaultQueryMaxBufferedItemCount: Int = Integer.MAX_VALUE
  val DefaultResponseContinuationTokenLimitInKb = 10
  val DefaultWritingBatchSize_BulkInsert = 100000
  val DefaultWritingBatchSize_PointInsert = 500
  val DefaultWritingBatchDelayMs = 0
  val DefaultStreamingSlowSourceDelayMs = 1
  val DefaultBulkImport = true
  val DefaultBulkUpdate = false
  val DefaultMaxMiniBatchUpdateCount = 500
  val DefaultAdlUseGuidForId = true
  val DefaultAdlUseGuidForPk = true
  val DefaultAdlMaxFileCount: Int = Int.MaxValue

  val SinglePartitionCollectionOfferThroughput = 10000

  val DefaultInferStreamSchema = true

  val DefaultMaxConnectionPoolSize = 500

  val DefaultMaxMiniBatchImportSizeKB = 100

  val DefaultBulkImportMaxConcurrencyPerPartitionRange = 1

  val DefaultBaseMiniBatchRUConsumption = 2000

  val  DefaultConvertNestedDocsToNativeJsonFormat = false

  def parseParameters(parameters: Map[String, String]): Map[String, Any] = {
    parameters.map { case (x, v) => x -> v }
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
  val NONE: config.CachingMode.Value = Value("None")
  val CACHE: config.CachingMode.Value = Value("Cache")
  val REFRESH_CACHE: config.CachingMode.Value = Value("RefreshCache")
}
