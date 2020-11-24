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
package org.apache.spark.sql.cosmosdb.util

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, Period}
import java.util.concurrent.TimeUnit

import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.{Document, RequestOptions, ResourceResponse}
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBWriteStreamRetryPolicy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import rx.Observable

import scala.reflect.ClassTag

object StreamingUtils extends Serializable {

  def createDataFrameStreaming(df: DataFrame, schema: StructType, sqlContext: SQLContext): DataFrame = {

    val encoder = RowEncoder.apply(schema)
    val mappedRdd = df.rdd.map(row => {
      encoder.createSerializer.apply(row)
    })
    sqlContext.internalCreateDataFrame(mappedRdd, schema, isStreaming = true)
  }
}

class StreamingWriteTask extends Serializable with CosmosDBLoggingTrait {

  def importStreamingData[D: ClassTag](
    iter: Iterator[D],
    schemaOutput: Seq[Attribute],
    config: Config,
    retryPolicy: CosmosDBWriteStreamRetryPolicy): Unit = {

    val upsert: Boolean = config
      .getOrElse(CosmosDBConfig.Upsert, String.valueOf(CosmosDBConfig.DefaultUpsert))
      .toBoolean
    val maxWriteConcurrency = config
      .getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_PointInsert))
      .toInt

    val asyncConnection: AsyncCosmosDBConnection = new AsyncCosmosDBConnection(config)

    val startTime = LocalDateTime.now()

    logError(s"Max. write concurrency is $maxWriteConcurrency")

    val defaultRequestOptions : RequestOptions = null

    val schema = StructType.fromAttributes(schemaOutput)

    val result : Observable[ResourceResponse[Document]] = {
      if (upsert)
        retryPolicy.process(iter, schema, defaultRequestOptions, maxWriteConcurrency, asyncConnection.upsertDocument)
      else
        retryPolicy.process(iter, schema, defaultRequestOptions, maxWriteConcurrency, asyncConnection.createDocument)
    }

    val count = result.count().toBlocking.last()

    val latency = Math.abs(ChronoUnit.MILLIS.between(LocalDateTime.now(), startTime))
    logInfo(s"Batch of $count records written with latency $latency milliseconds")
  }
}
