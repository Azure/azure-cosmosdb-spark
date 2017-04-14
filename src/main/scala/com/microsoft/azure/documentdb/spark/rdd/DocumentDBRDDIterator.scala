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
package com.microsoft.azure.documentdb.spark.rdd

import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.spark._
import com.microsoft.azure.documentdb.spark.config.{Config, DocumentDBConfig}
import com.microsoft.azure.documentdb.spark.partitioner.DocumentDBPartition
import com.microsoft.azure.documentdb.spark.schema._
import org.apache.spark._
import org.apache.spark.sql.sources.Filter

object DocumentDBRDDIterator {

  // For verification purpose
  var lastFeedOptions: FeedOptions = _

}

class DocumentDBRDDIterator(
                             taskContext: TaskContext,
                             partition: DocumentDBPartition,
                             config: Config,
                             maxItems: Option[Long],
                             requiredColumns: Array[String],
                             filters: Array[Filter])
  extends Iterator[Document]
    with LoggingTrait {

  private var closed = false
  private var initialized = false
  private var itemCount: Long = 0

  lazy val reader: Iterator[Document] = {
    initialized = true
    var conn: DocumentDBConnection = new DocumentDBConnection(config)

    val feedOpts = new FeedOptions()
    val pageSize: Int = config
      .get[String](DocumentDBConfig.QueryPageSize)
      .getOrElse(DocumentDBConfig.DefaultPageSize.toString)
      .toInt
    feedOpts.setPageSize(pageSize)
    // Set target partition ID
    BridgeInternal.setFeedOptionPartitionKeyRangeId(feedOpts, partition.partitionKeyRangeId.toString)
    feedOpts.setEnableCrossPartitionQuery(true)
    DocumentDBRDDIterator.lastFeedOptions = feedOpts

    var queryString = FilterConverter.createQueryString(requiredColumns, filters)
    logDebug(s"DocumentDBRDDIterator::LazyReader, convert to predicate: $queryString")

    conn.queryDocuments(queryString, feedOpts)
  }

  // Register an on-task-completion callback to close the input stream.
  taskContext.addTaskCompletionListener((context: TaskContext) => closeIfNeeded())

  override def hasNext: Boolean = {
    if (maxItems != null && maxItems.isDefined && maxItems.get <= itemCount) {
      return false
    }
    !closed && reader.hasNext
  }

  override def next(): Document = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    itemCount = itemCount + 1
    reader.next()
  }

  def closeIfNeeded(): Unit = {
    if (!closed) {
      closed = true
      close()
    }
  }

  protected def close(): Unit = {
    if (initialized) {
      initialized = false
    }
  }
}
