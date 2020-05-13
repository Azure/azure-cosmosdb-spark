/**
  * The MIT License (MIT)
  * Copyright (c) 2020 Microsoft Corporation
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
package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor

/**
  * Case class used as an envelope of all the data being cached in teh Executors for usage
  * in CosmosDBConnection within.
  *
  * @param docClient                An instance of the DocumentClient used by the CosmosDBConnection
  * @param bulkExecutor             An instance of the BulkExecutor instance used by the CosmosDBConnection (optional).
  *                                 Will be lazily initialized only when needed.
  * @param containerMetadata        Container metadata for the target container. Only immutable information is stored
  *                                 here - so does not to be refreshed or updated on partition splits etc. (optional).
  *                                 Will be lazily initialized only when needed.
  * @param databaseMetadata         Database metadata for the database. Only immutable information is stored
  *                                 here - so does not to be refreshed or updated on partition splits etc. (optional)
  *                                 Will be lazily initialized only when needed.
  * @param maxAvailableThroughput   The effectively available provisioned throughput. It
  *                                 is calculated by determining whether the throughput is provisioned at the container
  *                                 or database level and the configured max. throughput that may be used for bulk
  *                                 operations. This information is mutable - and is going to be refreshed regularly to
  *                                 be bale to react and if possible increase throughput used by bulk executor if
  *                                 additional throughput is getting provisioned.
  */
private[spark] case class ClientCacheEntry(
                            docClient: DocumentClient,
                            bulkExecutor: Option[DocumentBulkExecutor],
                            containerMetadata: Option[ContainerMetadata],
                            databaseMetadata: Option[DatabaseMetadata],
                            maxAvailableThroughput: Option[Int]) {

  def getLogMessage : String = {
    val clientMsg = s"DocumentClient#${docClient.hashCode()}"
    
    val bulkExecutorMsg : String = if (bulkExecutor.isDefined) {
      s", BulkExecutor#${bulkExecutor.get.hashCode()}"
    } else {
      ""
    }

    val containerMsg : String = if (containerMetadata.isDefined) {
      s", ContainerMetadata#${containerMetadata.get.hashCode()}['${containerMetadata.get.id}']"
    } else {
      ""
    }

    val databaseMsg : String = if (databaseMetadata.isDefined) {
      s", DatabaseMetadata#${databaseMetadata.get.hashCode()}['${databaseMetadata.get.id}']"
    } else {
      ""
    }

    val throughputMsg : String  = if (maxAvailableThroughput.isDefined) {
      s", MaxAvailableThroughput#${maxAvailableThroughput.get.hashCode()}['${maxAvailableThroughput.get}']"
    } else {
      ""
    }

    s"ClientCacheEntry#${hashCode()}($clientMsg$bulkExecutorMsg$containerMsg$databaseMsg$throughputMsg)"
  }
}