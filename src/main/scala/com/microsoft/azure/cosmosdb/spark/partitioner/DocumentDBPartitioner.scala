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
package com.microsoft.azure.cosmosdb.spark.partitioner

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.schema.FilterConverter
import com.microsoft.azure.cosmosdb.spark.{DocumentDBConnection, LoggingTrait}
import org.apache.spark.sql.sources.Filter

class DocumentDBPartitioner() extends Partitioner[DocumentDBPartition] with LoggingTrait {

  /**
    * @param config Partition configuration
    */
  override def computePartitions(config: Config): Array[DocumentDBPartition] = {
    var connection: DocumentDBConnection = new DocumentDBConnection(config)
    var partitionKeyRanges = connection.getAllPartitions
    logDebug(s"DocumentDBPartitioner: This DocumentDB has ${partitionKeyRanges.length} partitions")
    Array.tabulate(partitionKeyRanges.length){
      i => DocumentDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt)
    }
  }

  def computePartitions(config: Config,
                       requiredColumns: Array[String] = Array(),
                       filters: Array[Filter] = Array()): Array[DocumentDBPartition] = {
    var connection: DocumentDBConnection = new DocumentDBConnection(config)
    var query: String = FilterConverter.createQueryString(requiredColumns, filters)
    var partitionKeyRanges = connection.getAllPartitions(query)
    logDebug(s"DocumentDBPartitioner: This DocumentDB has ${partitionKeyRanges.length} partitions")
    Array.tabulate(partitionKeyRanges.length){
      i => DocumentDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt)
    }
  }
}
