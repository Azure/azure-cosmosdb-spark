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

import com.microsoft.azure.documentdb._
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object CosmosDBDefaults {
  def apply(): CosmosDBDefaults = new CosmosDBDefaults()
}

class CosmosDBDefaults extends LoggingTrait {

  val CosmosDBEndpoint: String = System.getProperty("CosmosDBEndpoint")
  val CosmosDBKey: String = System.getProperty("CosmosDBKey")
  val DatabaseName = "cosmosdb-spark-connector-test"
  val PartitionKeyName = "pkey"

  var collectionName: String = _

  private val connectionPolicy = ConnectionPolicy.GetDefault()
  connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps)
  connectionPolicy.setUserAgentSuffix(Constants.userAgentSuffix)

  lazy val documentDBClient = {
    var client = new DocumentClient(CosmosDBEndpoint, CosmosDBKey,
      connectionPolicy,
      ConsistencyLevel.Session)
    client
  }

  def getDocumentClient(): DocumentClient = documentDBClient

  def getSparkConf(colName: String): SparkConf = {
    collectionName = colName
    new SparkConf()
      .setMaster("local")
      .setAppName("CosmosDBSparkConnector")
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.sql.allowMultipleContexts", "false")
      .set("spark.app.id", "CosmosDBSparkConnector")
      .set("spark.cosmosdb.endpoint", CosmosDBEndpoint)
      .set("spark.cosmosdb.database", DatabaseName)
      .set("spark.cosmosdb.masterkey", CosmosDBKey)
      .set("spark.cosmosdb.collection", collectionName)
  }

  def isCosmosDBOnline(): Boolean = true

  def createDatabase(databaseName: String): Unit = {
    var database: Database = new Database()
    database.setId(databaseName)
    try {
      documentDBClient.createDatabase(database, null)
      logInfo(s"Created collection with Id ${database.getId}")
    } catch {
      case NonFatal(e) => logError(s"Failed to create database '$databaseName'", e)
    }
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
    partitionKeyDef.setPaths(List(s"/$PartitionKeyName").asJavaCollection)
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
