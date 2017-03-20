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
import com.microsoft.azure.documentdb.internal.directconnectivity.HttpClientFactory
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object DocumentDBDefaults {
  def apply(): DocumentDBDefaults = new DocumentDBDefaults()
}

class DocumentDBDefaults extends LoggingTrait {

  // local emulator
  val EMULATOR_ENDPOINT: String = "https://localhost:443/"
  val EMULATOR_MASTERKEY: String = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
  val DATABASE_NAME = "documentdb-spark-connector-test"

  var collectionName: String = _

  private val connectionPolicy = ConnectionPolicy.GetDefault()
  connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps)
  connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix)

  lazy val documentDBClient = {
    var client = new DocumentClient(EMULATOR_ENDPOINT, EMULATOR_MASTERKEY,
      connectionPolicy,
      ConsistencyLevel.Session)
    client
  }

  def getDocumentClient(): DocumentClient = documentDBClient

  def getSparkConf(colName: String): SparkConf = {
    collectionName = colName
    new SparkConf()
      .setMaster("local")
      .setAppName("DocumentDBSparkConnector")
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.sql.allowMultipleContexts", "false")
      .set("spark.app.id", "DocumentDBSparkConnector")
      .set("spark.documentdb.endpoint", EMULATOR_ENDPOINT)
      .set("spark.documentdb.database", DATABASE_NAME)
      .set("spark.documentdb.masterkey", EMULATOR_MASTERKEY)
      .set("spark.documentdb.collection", collectionName)
  }

  def isDocumentDBOnline(): Boolean = true

  def createDatabase(databaseName: String): Unit = {
    var database: Database = new Database()
    database.setId(databaseName)
    documentDBClient.createDatabase(database, null)
    logInfo(s"Created collection with Id ${database.getId}")
  }

  def deleteDatabase(databaseName: String): Unit = {
    var databaseLink = "dbs/" + databaseName
    try {
      documentDBClient.deleteDatabase(databaseLink, null)
      logInfo(s"Deleted collection with link '$databaseLink'")
    } catch {
      case NonFatal(e) => logError(s"Failed to delete database '$databaseLink'", e)
    }
  }

  def createCollection(databaseName: String, collectionName: String): Unit = {
    var collection: DocumentCollection = new DocumentCollection()
    collection.setId(collectionName)

    // Partition key definition
    val partitionKeyDef = new PartitionKeyDefinition
    partitionKeyDef.setPaths(List("/pkey").asJavaCollection)
    collection.setPartitionKey(partitionKeyDef)

    // Indexing paths
    // The String Range index is there to test String functions such as StartsWith, EndsWith
	  var defaultPath: IncludedPath = new IncludedPath()
    defaultPath.setPath("/")
    val defaultIndex: java.util.Collection[Index] = ArrayBuffer[Index](Index.Range(DataType.Number), Index.Range(DataType.String)).asJava
    defaultPath.setIndexes(defaultIndex)

    var indexingPolicy: IndexingPolicy = new IndexingPolicy()
    indexingPolicy.setIncludedPaths(List(defaultPath).asJavaCollection)
    collection.setIndexingPolicy(indexingPolicy)

    var requestOptions: RequestOptions = new RequestOptions()
    requestOptions.setOfferThroughput(10100)

    documentDBClient.createCollection("dbs/" + databaseName, collection, requestOptions)
    logInfo(s"Created collection with Id ${collection.getId}")
  }

  def deleteCollection(databaseName: String, collectionName: String): Unit = {
    var collectionLink = "dbs/" + databaseName + "/colls/" + collectionName
    try {
      documentDBClient.deleteCollection(collectionLink, null)
      logInfo(s"Deleted collection with link '$collectionLink'")
    } catch {
      case NonFatal(e) => logError(s"Failed to delete collection '$collectionLink'", e)
    }
  }

  def loadSampleData(collectionName: String, name: String): Unit = {
  }

  def loadSampleData(collectionName: String, sizeInMB: Int): Unit = {
  }

}
