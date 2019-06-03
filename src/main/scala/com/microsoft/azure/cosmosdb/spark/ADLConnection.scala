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

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider
import com.microsoft.azure.datalake.store.{ADLStoreClient, DirectoryEntry}
import com.microsoft.azure.documentdb.{Document, FeedOptions}

object ADLConnection {
  def markAdlFileProcessed(hdfsUtils: HdfsUtils,
                           adlFileCheckpointPath: String,
                           adlFilePath: String,
                           writingBatchId: String): Unit = {
    val id = s"${writingBatchId}_${HdfsUtils.filterFilename(adlFilePath)}"
    hdfsUtils.write(adlFileCheckpointPath, id, adlFilePath)
  }

  def markAdlFileStatus(connection: CosmosDBConnection,
                        collectionLink: String,
                        adlFilePath: String,
                        writingBatchId: String,
                        isInProgress: Boolean,
                        isComplete: Boolean): Unit = {
    val d = new Document()
    d.setId(URLEncoder.encode(adlFilePath, StandardCharsets.UTF_8.name()))
    d.set("name", adlFilePath)
    d.set("createDate", Instant.now.toString)
    d.set("isInProgress", isInProgress)
    d.set("isComplete", isComplete)
    d.set("batchId", writingBatchId)
    connection.upsertDocument(collectionLink, d, null)
  }

  def isAdlFileProcessed(hdfsUtils: HdfsUtils,
                         adlFileCheckpointPath: String,
                         adlFilePath: String,
                         writingBatchId: String): Boolean = {
    val id = s"${writingBatchId}_${HdfsUtils.filterFilename(adlFilePath)}"
    hdfsUtils.fileExist(adlFileCheckpointPath, id)
  }

  def isAdlFileProcessed(connection: CosmosDBConnection,
                         collectionLink: String,
                         adlFilePath: String,
                         writingBatchId: String): Boolean = {
    val encodedFilePath = URLEncoder.encode(adlFilePath, StandardCharsets.UTF_8.name())
    val query = s"SELECT VALUE 1 FROM c WHERE c.id = '$encodedFilePath' and c.batchId = '$writingBatchId' and c.isComplete = true"
    val response = connection.queryDocuments(collectionLink, query, null)
    response.hasNext
  }

  def getUnprocessedFiles(connection: CosmosDBConnection,
                          collectionLink: String): java.util.HashSet[String] = {
    val feedOptions = new FeedOptions()
    val response = connection.queryDocuments(collectionLink, feedOptions)
    var files = new java.util.HashSet[String]()
    while (response.hasNext) {
      val item = response.next()
      files.add(item.getString("name"))
    }
    files
  }

  def cleanUpProgress(hdfsUtils: HdfsUtils, adlFileCheckpointPath: String): Unit = {
    hdfsUtils.deleteFile(adlFileCheckpointPath)
  }
}

private[spark] case class ADLConnection (config: Config) extends CosmosDBLoggingTrait with Serializable {

  private val adlAccountFqdn = config.get[String](CosmosDBConfig.adlAccountFqdn).get
  private val adlClientId = config.get[String](CosmosDBConfig.adlClientId).get
  private val adlAuthTokenEndpoint = config.get[String](CosmosDBConfig.adlAuthTokenEndpoint).get
  private val adlClientKey = config.get[String](CosmosDBConfig.adlClientKey).get
  private val adlDataFolder = config.get[String](CosmosDBConfig.adlDataFolder).get
  private val adlIdField = config.get[String](CosmosDBConfig.adlIdField)
  private val adlPkField = config.get[String](CosmosDBConfig.adlPkField)
  private val adlCosmosDBCollectionPk = config.get[String](CosmosDBConfig.adlCosmosDbDataCollectionPkValue).get
  private val adlUseGuidForId = config.get[String](CosmosDBConfig.adlUseGuidForId)
    .getOrElse(CosmosDBConfig.DefaultAdlUseGuidForId.toString)
    .toBoolean
  private val adlUseGuidForPk = config.get[String](CosmosDBConfig.adlUseGuidForPk)
    .getOrElse(CosmosDBConfig.DefaultAdlUseGuidForPk.toString)
    .toBoolean

  private final val MaxEntriesToRetrieve = 100000
  private final val InitAdlFileRowCount = 800000

  @transient private var adlClient: ADLStoreClient = _

  private lazy val client: ADLStoreClient = {
    if (adlClient == null) {
      val provider = new ClientCredsTokenProvider(adlAuthTokenEndpoint, adlClientId, adlClientKey)
      adlClient = ADLStoreClient.createClient(adlAccountFqdn, provider)
    }
    adlClient
  }

  def getFiles(adlFolder: String): java.util.List[String] = {
    val list: java.util.List[DirectoryEntry] = client.enumerateDirectory(adlFolder, MaxEntriesToRetrieve)
    val files: java.util.List[String] = new java.util.ArrayList[String]
    logDebug(s"Found ${list.size} files in ADL folder")
    for (i <- 0 until list.size()) {
      files.add(list.get(i).fullName)
    }
    files
  }

  def getFiles: java.util.List[String] = {
    getFiles(adlDataFolder)
  }

  def readAdlFile(fileName: String): java.util.List[Document] = {
    logDebug(s"Loading Adl file $fileName")
    val startTime: Long = System.currentTimeMillis
    val records: java.util.List[Document] = new java.util.ArrayList[Document](InitAdlFileRowCount)
    val in: InputStream = client.getReadStream(fileName)
    var count = 0
    try {
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(in))
      try {
        var line: String = reader.readLine()
        while (line != null) {
          val doc: Document = new Document(line)
          val partitionKey: String = adlCosmosDBCollectionPk
          if (adlIdField.isDefined && !adlIdField.get.isEmpty) doc.set("id", doc.get(adlIdField.get))
          if (adlPkField.isDefined && !adlPkField.get.isEmpty) doc.set(partitionKey, doc.get(adlPkField.get))
          if (adlUseGuidForId) doc.set("id", UUID.randomUUID.toString)
          if (adlUseGuidForPk) doc.set(partitionKey, UUID.randomUUID.toString)
          records.add(doc)
          line = reader.readLine()

          count = count + 1
          if (count % 100000 == 0) {
            logInfo(s"Read $count Document")
          }
        }
      } finally if (reader != null) reader.close()
    }
    finally if (in != null) in.close()
    logDebug(s"Total loaded records : ${records.size}")
    val endTime: Long = System.currentTimeMillis
    val totalTime: Long = endTime - startTime
    logDebug(s"Adl file loading execution time in seconds: ${totalTime / 1000}")
    records
  }
}
