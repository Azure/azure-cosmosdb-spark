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
import com.microsoft.azure.cosmosdb.spark.{ADLConnection, ADLFilePartition, CosmosDBConnection, LoggingTrait}
import org.apache.spark.Partition
import org.apache.spark.sql.sources.Filter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CosmosDBPartitioner() extends Partitioner[Partition] with LoggingTrait {

  /**
    * @param config Partition configuration
    */
  override def computePartitions(config: Config): Array[Partition] = {
    var connection: CosmosDBConnection = new CosmosDBConnection(config)
    var partitionKeyRanges = connection.getAllPartitions
    logDebug(s"CosmosDBPartitioner: This CosmosDB has ${partitionKeyRanges.length} partitions")
    Array.tabulate(partitionKeyRanges.length){
      i => CosmosDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt)
    }
  }

  def computePartitions(config: Config,
                        requiredColumns: Array[String] = Array(),
                        filters: Array[Filter] = Array(),
                        hadoopConfig: mutable.Map[String, String]): Array[Partition] = {
    val adlImport = config.get(CosmosDBConfig.adlAccountFqdn).isDefined
    var connection: CosmosDBConnection = new CosmosDBConnection(config)
    connection.reinitializeClient()

    if (adlImport) {
      // ADL source
      val hdfsUtils = new HdfsUtils(hadoopConfig.toMap)
      val adlConnection: ADLConnection = ADLConnection(config)
      val adlFiles = adlConnection.getFiles
      val adlCheckpointPath = config.get(CosmosDBConfig.adlFileCheckpointPath)
      val adlCosmosDBFileStoreCollection = config.get(CosmosDBConfig.CosmosDBFileStoreCollection)
      val writingBatchId = config.get[String](CosmosDBConfig.WritingBatchId)
      val adlMaxFileCount = config.get(CosmosDBConfig.adlMaxFileCount)
        .getOrElse(CosmosDBConfig.DefaultAdlMaxFileCount.toString)
        .toInt
      logDebug(s"The Adl folder has ${adlFiles.size()} files")
      val partitions = new ListBuffer[ADLFilePartition]
      var partitionIndex = 0
      var i = 0
      while (i < adlFiles.size() && partitionIndex < adlMaxFileCount) {
        var processed = true
        if (adlCheckpointPath.isDefined) {
          processed = ADLConnection.isAdlFileProcessed(hdfsUtils, adlCheckpointPath.get, adlFiles.get(i), writingBatchId.get)
        } else if (adlCosmosDBFileStoreCollection.isDefined) {
          val dbName = config.get[String](CosmosDBConfig.Database).get
          val collectionLink = s"/dbs/$dbName/colls/${adlCosmosDBFileStoreCollection.get}"
          processed = ADLConnection.isAdlFileProcessed(connection, collectionLink, adlFiles.get(i), writingBatchId.get)
        }
        if (!processed) {
          partitions += ADLFilePartition(partitionIndex, adlFiles.get(i))
          partitionIndex += 1
        }
        i += 1
      }
      partitions.toArray
    } else {
      // CosmosDB source
      var query: String = FilterConverter.createQueryString(requiredColumns, filters)
      var partitionKeyRanges = connection.getAllPartitions(query)
      logDebug(s"CosmosDBPartitioner: This CosmosDB has ${partitionKeyRanges.length} partitions")
      Array.tabulate(partitionKeyRanges.length) {
        i => CosmosDBPartition(i, partitionKeyRanges.length, partitionKeyRanges(i).getId.toInt)
      }
    }
  }
}
