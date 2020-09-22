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
package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.control.Breaks._

private[spark] case class CosmosDBConnection(config: Config) extends CosmosDBLoggingTrait with Serializable {
  private lazy val collectionLink: String =
    CosmosDBConnectionCache.getOrReadContainerMetadata(clientConfig).selfLink
  private val maxPagesPerBatch =
    config.getOrElse[String](CosmosDBConfig.ChangeFeedMaxPagesPerBatch, CosmosDBConfig.DefaultChangeFeedMaxPagesPerBatch.toString).toInt
  private val clientConfig = ClientConfiguration(config)

  def getCollectionLink: String = {
    collectionLink
  }

  def reinitializeClient(): Unit = {
    CosmosDBConnectionCache.reinitializeClient(clientConfig)
  }

  def getAllPartitions: List[PartitionKeyRange] = {
    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val ranges = documentClient.readPartitionKeyRanges(collectionLink, null.asInstanceOf[FeedOptions])
    getListFromFeedResponse(ranges)
  }

  def getDocumentBulkImporter: DocumentBulkExecutor = {
    CosmosDBConnectionCache.getOrCreateBulkExecutor(clientConfig)
  }

  def getPartitionKeyDefinition: PartitionKeyDefinition = {
    CosmosDBConnectionCache.getPartitionKeyDefinition(clientConfig)
  }

  def queryDocuments(queryString: String,
                     feedOpts: FeedOptions): Iterator[Document] = {

    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val feedResponse: FeedResponse[Document] = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    getIteratorFromFeedResponse(feedResponse)
  }

  def queryDocuments(collectionLink: String, queryString: String,
                     feedOpts: FeedOptions): Iterator[Document] = {
    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val feedResponse: FeedResponse[Document] = documentClient.queryDocuments(collectionLink, new SqlQuerySpec(queryString), feedOpts)
    getIteratorFromFeedResponse(feedResponse)
  }

  def readDocuments(feedOptions: FeedOptions): Iterator[Document] = {
    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val resp: FeedResponse[Document] = documentClient.readDocuments(collectionLink, feedOptions)
    getIteratorFromFeedResponse(resp)
  }

/**
   * Takes the results from a FeedResponse and puts them in a standard List by fully draining the query.
   * The FeedResponse otherwise hides a lot of extra fields behind the Iterator[T] interface that would still
   * need to be serialized when being collected on the driver.
   * @param response
   * @return
   */
  private def getListFromFeedResponse[T <: com.microsoft.azure.documentdb.Resource : ClassTag](
    response: FeedResponse[T]): List[T] = {
    
    response
        .getQueryIterator
        .toList
  }

  /**
   * Takes the results from a FeedResponse and puts them in a standard List if 
   * there is no continuation token. In this case the FeedResponse would hide a lot 
   * of extra fields behind the Iterator[T] interface that would still
   * need to be serialized when being collected on the driver.
   * If the FeedResponse contains a continuation the query iterator is returned so
   * that the query results can be drained by the driver.
   * @param response
   * @return
   */
  private def getIteratorFromFeedResponse[T <: com.microsoft.azure.documentdb.Resource : ClassTag](
    response: FeedResponse[T]): Iterator[T] = {

    val responseContinuation:String = response.getResponseContinuation
    if (responseContinuation == null || responseContinuation.isEmpty) {
      logDebug(s"CosmosDBConnection.getIteratorFromFeedResponse -- No continuation - returning simple list")
      val responseList:List[T]  = response
        .getQueryIterator
        .toList
      responseList.iterator
    } else {
      logDebug(s"CosmosDBConnection.getIteratorFromFeedResponse -- With continuation - returning query iterator")
      val responseIterator:Iterator[T]  = response
        .getQueryIterator
      responseIterator      
    }
  }

  def readChangeFeed(changeFeedOptions: ChangeFeedOptions,
                     isStreaming: Boolean,
                     shouldInferStreamSchema: Boolean,
                     updateTokenFunc: (String, String, String) => Unit
                    ): Iterator[Document] = {

    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val partitionId = changeFeedOptions.getPartitionKeyRangeId

    logDebug(s"--> readChangeFeed, PageSize: ${changeFeedOptions.getPageSize.toString}, ContinuationToken: ${changeFeedOptions.getRequestContinuation}, StartFromBeginning: ${changeFeedOptions.isStartFromBeginning}, StartDateTime: ${changeFeedOptions.getStartDateTime}, PartitionId: $partitionId, ShouldInferSchema: ${shouldInferStreamSchema.toString}")

    // The ChangeFeed API in the SDK allows accessing the continuation token
    // from the latest HTTP Response
    // This is not sufficient to build a correct continuation token when
    // the "ChangeFeedMaxPagesPerBatch" limit is reached, because "blocks" that
    // can be retrieved from the SDK can span two or more underlying pages. So the first records in 
    // the block can only be retrieved with the previous continuation token - the last
    // records would have the continuation token of the latest HTTP response that is retrievable
    // The variables below are used to store context necessary to form a continuation token
    // that allows bookmarking an individual record within the changefeed
    // The continuation token that would need to be used to safely allow retrieving changerecords
    // after a bookmark in the form of <blockStartContinuation>|<lastProcessedIdBookmark>
    // Meaning the <blockStartContinuation> needs to be at a previous or the same page as the change record
    // document with Id <lastProcessedIdBookmark>

    // Indicator whether we found the first not yet processed change record
    var foundBookmark = true

    // The id of the last document that has been processed and returned to the caller
    var lastProcessedIdBookmark = ""

    // The original continuation that has been passed to this method by the caller
    val originalContinuation = changeFeedOptions.getRequestContinuation
    var currentContinuation = originalContinuation

    // The next continuation token that is returned to the caller to continue
    // processing the change feed
    var nextContinuation = changeFeedOptions.getRequestContinuation
    if (currentContinuation != null &&
      currentContinuation.contains("|")) {
      val continuationFragments = currentContinuation.split('|')
      if (continuationFragments.size <= 2) {
        lastProcessedIdBookmark = continuationFragments(1)
      }
      // handle the case in which "id" contains "|" character included in it
      else {
        lastProcessedIdBookmark = currentContinuation.substring(continuationFragments(0).length + 1)
      }
      currentContinuation = continuationFragments(0)
      changeFeedOptions.setRequestContinuation(currentContinuation)
      foundBookmark = false
    }

    // The continuation token that would need to be used to safely allow retrieving changerecords
    // after a bookmark in the form of <blockStartContinuation>|<lastProcessedIdBookmark>
    // Meaning the <blockStartContinuation> needs to be at a previous or the same page as the change record
    // document with Id <lastProcessedIdBookmark>
    var previousBlockStartContinuation = currentContinuation

    // blockStartContinuation is used as a place holder to store the feedResponse.getResponseContinuation()
    // of the previous HTTP response to be able to apply it to previousBlockStartContinuation
    // accordingly
    var blockStartContinuation = currentContinuation

    // This method can result in reading the next page of the changefeed and changing the continuation token header
    val feedResponse = documentClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)
    logDebug(s"    readChangeFeed.InitialResponseContinuation: ${feedResponse.getResponseContinuation}")

    // If processing from the beginning (no continuation token passed into this method)
    // it is safe to increase previousBlockStartContinuation here because we always at least return
    // one page
    if (Option(currentContinuation).getOrElse("").isEmpty) {
      blockStartContinuation = feedResponse.getResponseContinuation
      previousBlockStartContinuation = blockStartContinuation
    }

    if (isStreaming) {
      var pageCount = 0
      // In streaming scenario, the change feed need to be materialized in order to get the information of the continuation token
      val cfDocuments: ListBuffer[Document] = new ListBuffer[Document]
      breakable {
        // hasNext can result in reading the next page of the changefeed and changing the continuation token header
        while (feedResponse.getQueryIterator.hasNext) {
          logDebug(s"    readChangeFeed.InWhile ContinuationToken: $blockStartContinuation")
          // fetchNextBlock can result in reading the next page of the changefeed and changing the continuation token header
          val feedItems = feedResponse.getQueryIterable.fetchNextBlock()

          for (feedItem <- feedItems) {
            if (!foundBookmark) {
              // to capture updates to existing docs when maxPagesPerBatch is being used
              // adds the updated docs to the list while searching for the bookemarked Id from the prior batch
              if (feedItem.getInt("_lsn") > currentContinuation.toInt) {
                cfDocuments.add(feedItem)
              }

              if (feedItem.get("id") == lastProcessedIdBookmark) {
                logDebug("    readChangeFeed.FoundBookmarkDueToIdMatch")
                foundBookmark = true
              }
            }
            else {
              if (shouldInferStreamSchema) {
                cfDocuments.add(feedItem)
              }
              else {
                val streamDocument: Document = new Document()
                streamDocument.set("body", feedItem.toJson)
                streamDocument.set("id", feedItem.get("id"))
                streamDocument.set("_rid", feedItem.get("_rid"))
                streamDocument.set("_self", feedItem.get("_self"))
                streamDocument.set("_etag", feedItem.get("_etag"))
                streamDocument.set("_attachments", feedItem.get("_attachments"))
                streamDocument.set("_ts", feedItem.get("_ts"))

                cfDocuments.add(streamDocument)
              }
            }
          }
          logDebug(s"Receiving ${cfDocuments.length.toString} change feed items ${if (cfDocuments.nonEmpty) cfDocuments.head}")

          if (cfDocuments.nonEmpty) {
            pageCount += 1
          }

          if (pageCount >= maxPagesPerBatch) {
            if (maxPagesPerBatch > 1) {
              nextContinuation = previousBlockStartContinuation + "|" + feedItems.last.get("id")
            } else {
              // when maxPagesPerBatch = 1, the nextContinuation needs to advance to the next continuation
              nextContinuation = feedResponse.getResponseContinuation + "|" + feedItems.last.get("id")
            }

            logDebug(s"    readChangeFeed.MaxPageCountExceeded NextContinuation: $nextContinuation")
            break
          }
          else {
            // next Continuation Token is plain and simple the same as the latest HTTP response
            // Expected when all records of the current page have been processed
            // Will only get returned to the caller when the changefeed has been processed completely
            // as a continuation token that the caller can use afterwards to see whether the changefeed 
            // contains new change record documents
            nextContinuation = feedResponse.getResponseContinuation

            previousBlockStartContinuation = blockStartContinuation
            blockStartContinuation = nextContinuation

            logDebug(s"    readChangeFeed.EndInWhile NextContinuation: $nextContinuation, blockStartContinuation: $blockStartContinuation, previousBlockStartContinuation: $previousBlockStartContinuation")
          }
        }
      }

      // set nextContinuation if the given partition does not have an existing checkpoint file and the "startFromBeginning" is False
      if (nextContinuation == null || nextContinuation.isEmpty){
        nextContinuation = feedResponse.getResponseContinuation
      }

      logDebug(s"<-- readChangeFeed, Count: ${cfDocuments.length.toString}, NextContinuation: $nextContinuation")

      updateTokenFunc(originalContinuation, nextContinuation, partitionId)
      logDebug(s"changeFeedOptions.partitionKeyRangeId = $partitionId, continuation = $originalContinuation, new token = $nextContinuation")
      cfDocuments.iterator()
    } else {
      // next Continuation Token is plain and simple when not using Streaming because
      // all records will be processed. The parameter 'maxPagesPerBatch' is irrelevant
      // in this case - so there doesn't need to be any suffix in the continuation token returned
      nextContinuation = feedResponse.getResponseContinuation
      logDebug(s"<-- readChangeFeed, Non-Streaming, NextContinuation: $nextContinuation")
      new ContinuationTokenTrackingIterator[Document](
        feedResponse,
        updateTokenFunc,
        (msg: String) => logDebug(msg),
        partitionId
      )
    }
  }

  def upsertDocument(collectionLink: String,
                     document: Document,
                     requestOptions: RequestOptions): Unit = {
    logTrace(s"Upserting document $document")
    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    documentClient.upsertDocument(collectionLink, document, requestOptions, false)
  }

  def isDocumentCollectionEmpty: Boolean = {
    logDebug(s"Reading collection $collectionLink")
    val requestOptions = new RequestOptions
    requestOptions.setPopulateQuotaInfo(true)
    val documentClient = CosmosDBConnectionCache.getOrCreateClient(clientConfig)
    val response = documentClient.readCollection(collectionLink, requestOptions)
    response.getDocumentCountUsage == 0
  }
}

