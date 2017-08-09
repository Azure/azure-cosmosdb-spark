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

import com.microsoft.azure.documentdb.internal.directconnectivity.HttpClientFactory
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

trait RequiresCosmosDB extends FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with LoggingTrait {

  val cosmosDBDefaults: CosmosDBDefaults = CosmosDBDefaults()

  private var sparkContext: SparkContext = _

  def sparkConfa: SparkConf = sparkConf(getTestCollectionName)

  def sparkConf(collectionName: String): SparkConf = cosmosDBDefaults.getSparkConf(collectionName)

  /**
    * Test against a set SparkContext
    *
    * @param testCode the test case
    */
  def withSparkContext()(testCode: SparkContext => Any) {
    try {
      logInfo(s"Running Test: '$suiteName.$getTestCollectionName'")
      testCode(sparkContext)
    } finally {
    }
  }

  /**
    * Test against a set Spark Session
    *
    * @param testCode the test case
    */
  def withSparkSession()(testCode: SparkSession => Any) {
    try {
      logInfo(s"Running Test: '$suiteName.$getTestCollectionName'")
      testCode(createOrGetDefaultSparkSession(sparkContext)) // "loan" the fixture to the test
    } finally {
    }
  }

  override def beforeAll(): Unit = {
    // if running against localhost emulator
    HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true

    cosmosDBDefaults.createDatabase(cosmosDBDefaults.DatabaseName)
  }

  override def beforeEach(): Unit = {
    // this is run first
    // most of the initializations needed are moved to withFixture to use the test name
  }

  override def afterEach(): Unit = {
    cosmosDBDefaults.deleteCollection(cosmosDBDefaults.DatabaseName, getTestCollectionName)

    sparkContext.stop()
    sparkContext = null
  }

  def createOrGetDefaultSparkSession(sc: SparkContext): SparkSession = {
    var builder = SparkSession.builder().config(sc.getConf)
    val osName = System.getProperty("os.name")
    if (!StringUtils.isEmpty(osName) && osName.toLowerCase().contains("win")) {
      // The spark.sql.warehouse.dir parameter is to workaround an path issue with Spark on Windows
      builder.config("spark.sql.warehouse.dir", s"file:///${System.getProperty("user.dir")}")
    }
    builder.getOrCreate()
  }

  var testName = "UndefinedTestName"
  override def withFixture(test: NoArgTest): Outcome= {
    val testDescription = test.name.replaceAll("[^0-9a-zA-Z]", StringUtils.EMPTY)
    val maxDescriptionLength = 50
    testName = String.format("%s-%s-%s",
      suiteName,
      testDescription.substring(0, Math.min(maxDescriptionLength, testDescription.length)),
      System.currentTimeMillis().toString)

    // beforeEach test
    cosmosDBDefaults.createCollection(cosmosDBDefaults.DatabaseName, getTestCollectionName)
    sparkContext = new SparkContext(sparkConf(getTestCollectionName))

    super.withFixture(test)
  }

  /**
    * The collection name that is used for this test
    */
  def getTestCollectionName: String = {
    testName
  }
}
