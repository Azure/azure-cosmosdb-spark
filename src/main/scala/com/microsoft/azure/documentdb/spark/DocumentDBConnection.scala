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
package com.microsoft.azure.documentdb.spark

import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.internal.Paths
import com.microsoft.azure.documentdb.spark.config._

import scala.collection.JavaConversions._
import scala.language.implicitConversions

object DocumentDBConnection {

}

private[spark] case class DocumentDBConnection(config: Config) extends LoggingTrait with Serializable {
  private val databaseName = config.get[String](DocumentDBConfig.Database).get
  private val collectionName = config.get[String](DocumentDBConfig.Collection).get
  private val connectionMode = ConnectionMode.valueOf(config.get[String](DocumentDBConfig.ConnectionMode)
    .getOrElse(ConnectionMode.DirectHttps.toString))
  private val collectionLink = s"${Paths.DATABASES_PATH_SEGMENT}/$databaseName/${Paths.COLLECTIONS_PATH_SEGMENT}/$collectionName"
  
  @transient private var client: DocumentClient = _

  private def documentClient(): DocumentClient = {
    client match {
      case null => accquireClient()
      case _ => client
    }
  }

  private def accquireClient(): DocumentClient = {
    val connectionPolicy = ConnectionPolicy.GetDefault()
    connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps)
    connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix)

    val option = config.get[String](DocumentDBConfig.PreferredRegionsList)

    if (option.isDefined) {
      logWarning(s"DocumentDBConnection::Input preferred region list: ${option.get}")
      val preferredLocations = option.get.split(";").toSeq.map(_.trim)
      connectionPolicy.setPreferredLocations(preferredLocations)
    }

    client = new DocumentClient(
      config.get("EndPoint").getOrElse("endpoint"),
      config.get("Masterkey").getOrElse("masterkey"),
      connectionPolicy,
      ConsistencyLevel.Session)
    client
  }

  def getAllPartitions: Array [PartitionKeyRange] = {
    var ranges = documentClient().readPartitionKeyRanges(collectionLink, null)
    ranges.getQueryIterator.toArray
  }

  def queryDocuments (queryString : String,
        feedOpts : FeedOptions) : Iterator [Document] = {

    documentClient().queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts).getQueryIterable.iterator()
  }

  def upsertDocument(document: Document,
                     requestOptions: RequestOptions): Document = {
    try {
      logTrace(s"Upserting document $document")
      documentClient().upsertDocument(collectionLink, document, requestOptions, false).getResource
    } catch {
      case e: DocumentClientException =>
        logError("Failed to upsertDocument", e)
        null
    }
  }

  def createDocument(document: Document,
                     requestOptions: RequestOptions): Document = {
    try {
      logTrace(s"Creating document $document")
      documentClient().createDocument(collectionLink, document, requestOptions, false).getResource
    } catch {
      case e: DocumentClientException =>
        logError("Failed to createDocument", e)
        null
    }
  }

  def isDocumentCollectionEmpty: Boolean = {
    logDebug(s"Reading collection $collectionLink")
    var feedOptions: FeedOptions = new FeedOptions
    feedOptions.setPageSize(1)
    if (documentClient().readDocuments(collectionLink, feedOptions).getQueryIterator.hasNext) false else true
  }
}
