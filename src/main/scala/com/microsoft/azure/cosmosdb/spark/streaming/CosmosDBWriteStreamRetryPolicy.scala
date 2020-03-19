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

import com.microsoft.azure.cosmosdb.{Document, ResourceResponse, RequestOptions}
import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import rx.Observable
import scala.reflect.ClassTag
import scala.collection
import scala.collection.JavaConverters

class CosmosDBWriteStreamRetryPolicy(configMap: Map[String, String]) 
    extends CosmosDBLoggingTrait
    with Serializable
{
    val config = getConfig(configMap)
    val rnd = scala.util.Random
    private lazy val notificationHandler: CosmosDBWriteStreamPoisonMessageNotificationHandler = {
        getNotificationHandler(configMap)
    }

    def getConfig(configMap: Map[String, String]) : CosmosDBWriteStreamRetryPolicyConfig =
    {
        val retryPolicyKind = configMap
        .getOrElse(
            CosmosDBConfig.WriteStreamRetryPolicyKind,
            String.valueOf(CosmosDBConfig.DefaultWriteStreamRetryPolicyKind))
        
        val retryPolicyConfig = retryPolicyKind match {
            case kind if kind matches "(?i)NoRetries" => new NoRetriesCosmosDBWriteStreamRetryPolicyConfig()
            case kind if kind matches "(?i)Default" => new DefaultCosmosDBWriteStreamRetryPolicyConfig(configMap)
            case _ => new NoRetriesCosmosDBWriteStreamRetryPolicyConfig()
        }

        logError("Retry policy kind '" + retryPolicyKind + "' --> " + retryPolicyConfig)
        // TODO logDebug("Retry policy kind '" + retryPolicyKind + "' --> " + retryPolicyConfig)

        retryPolicyConfig
    }

    def getNotificationHandler(configMap: Map[String, String]) : CosmosDBWriteStreamPoisonMessageNotificationHandler =
    {
        new DefaultCosmosDBWriteStreamPoisonMessageNotificationHandler(configMap)
    }

    def process[D: ClassTag](iter: Iterator[D],
                schema: StructType,
                requestOptions: RequestOptions,
                maxWriteConcurrency: Integer,
                task: Function2[Document, RequestOptions, Observable[ResourceResponse[Document]]]): Observable[ResourceResponse[Document]] =
    {
        val maxRetries = this.config.getMaxTransientRetryCount()
        val maxRetryDelayInMs = this.config.getMaxTransientRetryDelayInMs()

        val retryUntil = Instant.now().get(ChronoField.MILLI_OF_SECOND) + this.config.getMaxTransientRetryDurationInMs();
        val attempts = new AtomicLong(0L);

        val itemConversionFunc = (item: D) => item match
        {
            case internalRow: InternalRow =>  new Document(CosmosDBRowConverter.internalRowToJSONObject(internalRow, schema).toString())
            case any => throw new IllegalStateException(s"InternalRow expected from structured stream")
        }

        val scalaItems : Iterable[D] = iter.toIterable

        CosmosDBWriteStreamRetryPolicyUtil.ProcessWithRetries[D](
            scalaItems,
            itemConversionFunc,
            requestOptions,
            task,
            this.config.isTransient _,
            (msg: String) => logError(msg),
            (throwable: Throwable, document: Document) => this.notificationHandler.onPoisonMessage(throwable, document),
            this.rnd,
            maxRetries,
            maxRetryDelayInMs,
            maxWriteConcurrency,
            retryUntil,
            attempts)
    } 
}