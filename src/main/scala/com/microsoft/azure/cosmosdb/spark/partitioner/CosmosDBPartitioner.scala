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
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.cosmosdb.spark.{CosmosDBConnection, CosmosDBLoggingTrait}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partition
import org.apache.spark.sql.sources.Filter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CosmosDBPartitioner(hadoopConfig: mutable.Map[String, String]) extends Partitioner[Partition] with CosmosDBLoggingTrait {

  /**
    * @param config Partition configuration
    */
  override def computePartitions(config: Config): Array[Partition] = {
    val connection: CosmosDBConnection = CosmosDBConnection(config, hadoopConfig)
    val partitionKeyRanges = connection.getAllPartitions
    logDebug(s"CosmosDBPartitioner: This CosmosDB has ${partitionKeyRanges.length} partitions")
    Array.tabulate(partitionKeyRanges.length){
      i => CosmosDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt, partitionKeyRanges(i).getParents)
    }
  }

  def computePartitions(config: Config, requiredColumns: Array[String] = Array()): Array[Partition] = {
    val connection: CosmosDBConnection = CosmosDBConnection(config, hadoopConfig)
    connection.reinitializeClient()

    // CosmosDB source
    val partitionKeyRanges = connection.getAllPartitions
    logInfo(s"CosmosDBPartitioner: This CosmosDB has ${partitionKeyRanges.length} partitions")
    Array.tabulate(partitionKeyRanges.length) {
      i => CosmosDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt, partitionKeyRanges(i).getParents)
    }
  }
}
