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
package com.microsoft.azure.cosmosdb.spark.schema

import java.io.File
import java.sql.{Date, Timestamp}
import java.util.concurrent.TimeUnit

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.rdd.{CosmosDBRDD, CosmosDBRDDIterator}
import com.microsoft.azure.cosmosdb.spark.streaming.{CosmosDBSinkProvider, CosmosDBSourceProvider}
import com.microsoft.azure.cosmosdb.spark.{RequiresCosmosDB, _}
import com.microsoft.azure.documentdb._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class SimpleDocument(id: String, pkey: Int, intString: String)

class CosmosDBDataFrameSpec extends RequiresCosmosDB {
  val documentCount = 100
  val simpleDocuments: IndexedSeq[SimpleDocument] = (1 to documentCount)
    .map(x => SimpleDocument(x.toString, x, (documentCount - x + 1).toString))

  // DataFrameWriter
  "DataFrameWriter" should "be easily created from a DataFrame and save to CosmosDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.cosmosDB()

    var cosmosDBRDD: CosmosDBRDD = sc.loadFromCosmosDB()
    cosmosDBRDD.map(x => x.getInt("pkey")).collect() should contain theSameElementsAs (1 to documentCount).toList
    cosmosDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs (1 to documentCount).toList

    // Create new documents and overwrite the previous ones
    sc.parallelize((1 to documentCount)
      .map(x => SimpleDocument(x.toString, x, ((documentCount - x + 1) + documentCount).toString)))
      .toDF().write.mode(SaveMode.Overwrite).cosmosDB()
    cosmosDBRDD = sc.loadFromCosmosDB()
    var expectedNewValues = (documentCount + 1 to documentCount * 2).toList
    cosmosDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs expectedNewValues
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    val config: Config = Config(sc)
    val databaseName: String = config.get(CosmosDBConfig.Database).get
    val collectionName: String = String.format("NewCollection-%s", System.currentTimeMillis().toString)
    cosmosDBDefaults.createCollection(databaseName, collectionName)
    var configMap: collection.Map[String, String] = config.asOptions
    configMap = configMap.updated(CosmosDBConfig.Collection, collectionName)
    val newConfig: Config = Config(configMap)

    sc.parallelize(simpleDocuments).toDF().write.cosmosDB(newConfig)
    var cosmosDBRDD: CosmosDBRDD = sc.loadFromCosmosDB(newConfig)
    cosmosDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs (1 to documentCount).toList

    cosmosDBDefaults.deleteCollection(databaseName, collectionName)
  }

  // DataFrameReader
  "DataFrameReader" should "should be easily created from SQLContext query" in withSparkContext() { sc =>
    sc.parallelize((1 to documentCount).map(x => new Document(s"{pkey: $x}"))).saveToCosmosDB()

    val sparkSession = createOrGetDefaultSparkSession(sc)

    val coll = sparkSession.sqlContext.read.cosmosDB()
    coll.createOrReplaceTempView("c")

    var query = "SELECT * FROM c "

    // Run DF query (count)
    val nanoPerSecond = 1e9
    var start = System.nanoTime()
    val df = sparkSession.sql(query)
    df.count() shouldBe documentCount
    var end = System.nanoTime()
    var durationSeconds = (end - start) / nanoPerSecond
    logDebug(s"df.count() took ${durationSeconds}s")

    // Run DF query (collect)
    start = System.nanoTime()
    df.rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should contain theSameElementsAs (1 to documentCount).toList
    end = System.nanoTime()
    durationSeconds = (end - start) / nanoPerSecond
    logDebug(s"df.query() took ${durationSeconds}s")
  }

  it should "read with query parameters" in withSparkSession() { sparkSession: SparkSession =>
    val count = 2000
    val testDocuments: IndexedSeq[SimpleDocument] = (1 to count)
      .map(x => SimpleDocument(x.toString, x, (count - x + 1).toString))

    import sparkSession.implicits._

    sparkSession.sparkContext.parallelize(testDocuments).toDF().write.cosmosDB()

    val host = CosmosDBDefaults().CosmosDBEndpoint
    val key = CosmosDBDefaults().CosmosDBKey
    val dbName = CosmosDBDefaults().DatabaseName
    val collName = getTestCollectionName

    val configMap = Map("Endpoint" -> host,
      "Masterkey" -> key,
      "Database" -> dbName,
      "Collection" -> collName,
      "SamplingRatio" -> "1.0")

    val readConfig = Config(configMap)

    var df = sparkSession.read.cosmosDB(readConfig)
    df.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect() should
      contain theSameElementsAs (1 to count).toList

    val serialReadConfig = Config(configMap.
      +((CosmosDBConfig.QueryPageSize, "-1")).
      +((CosmosDBConfig.QueryMaxDegreeOfParallelism, "1")))

    df = sparkSession.read.cosmosDB(serialReadConfig)
    df.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect() should
      contain theSameElementsAs (1 to count).toList

    val parallelReadConfig = Config(configMap.
      +((CosmosDBConfig.QueryPageSize, "100")).
      +((CosmosDBConfig.QueryMaxDegreeOfParallelism, Integer.MAX_VALUE.toString)).
      +((CosmosDBConfig.QueryMaxBufferedItemCount, Integer.MAX_VALUE.toString)))

    df = sparkSession.read.cosmosDB(parallelReadConfig)
    df.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect() should
      contain theSameElementsAs (1 to count).toList

    val dynamicReadConfig = Config(configMap.
      +((CosmosDBConfig.QueryPageSize, "-1")).
      +((CosmosDBConfig.QueryMaxDegreeOfParallelism, "-1")).
      +((CosmosDBConfig.QueryMaxBufferedItemCount, "-1")))

    df = sparkSession.read.cosmosDB(dynamicReadConfig)
    df.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect() should
      contain theSameElementsAs (1 to count).toList
  }

  it should "read documents change feed" in withSparkSession() { spark =>
    verifyReadChangeFeed(
      rollingChangeFeed = false,
      startFromTheBeginning = true,
      useNextToken = false,
      spark)
  }

  it should "read documents rolling change feed" in withSparkSession() { spark =>
    verifyReadChangeFeed(
      rollingChangeFeed = true,
      startFromTheBeginning = true,
      useNextToken = false,
      spark)
  }

  it should "read documents change feed not starting from the beginning" in withSparkSession() { spark =>
    verifyReadChangeFeed(
      rollingChangeFeed = false,
      startFromTheBeginning = false,
      useNextToken = false,
      spark)
  }

  it should "read documents rolling change feed not starting from the beginning" in withSparkSession() { spark =>
    verifyReadChangeFeed(
      rollingChangeFeed = true,
      startFromTheBeginning = false,
      useNextToken = false,
      spark)
  }

  it should "read documents change feed using next continuation token" in withSparkSession() { spark =>
    verifyReadChangeFeed(
      rollingChangeFeed = false,
      startFromTheBeginning = true,
      useNextToken = true,
      spark)
  }

  it should "read documents change feed not starting from the beginning and using next continuation token" in
    withSparkSession() { spark =>
      verifyReadChangeFeed(
        rollingChangeFeed = false,
        startFromTheBeginning = false,
        useNextToken = true,
        spark)
  }

  /**
    * Run tests to verify correctness of CosmosDB change feed scenarios
    *
    * @param rollingChangeFeed      indicates with this is rolling change feed (auto-update continuation token)
    * @param startFromTheBeginning  indicates whether change feed should be from the beginning
    * @param useNextToken           indicates that some of the time the next continuation token will be used
    * @param spark                  the sparkSession
    */
  def verifyReadChangeFeed(rollingChangeFeed: Boolean,
                           startFromTheBeginning: Boolean,
                           useNextToken: Boolean,
                           spark: SparkSession): Unit = {

    // INSERT some documents
    spark.sparkContext.parallelize((1 to documentCount).
      map(x => new Document(s"{ id: '$x', ${CosmosDBDefaults().PartitionKeyName}: '$x' }"))).
      saveToCosmosDB()

    val host = CosmosDBDefaults().CosmosDBEndpoint
    val key = CosmosDBDefaults().CosmosDBKey
    val dbName = CosmosDBDefaults().DatabaseName
    val collName = getTestCollectionName

    val checkpointPath = "./changefeedcheckpoint"

    FileUtils.deleteDirectory(new File(checkpointPath))

    var configMap = Map("Endpoint" -> host,
      "Masterkey" -> key,
      "Database" -> dbName,
      "Collection" -> collName,
      "ReadChangeFeed" -> "true",
      "ChangeFeedQueryName" -> s"$rollingChangeFeed $startFromTheBeginning $useNextToken",
      "ChangeFeedStartFromTheBeginning" -> startFromTheBeginning.toString,
      "ChangeFeedUseNextToken" -> useNextToken.toString,
      "RollingChangeFeed" -> rollingChangeFeed.toString,
      CosmosDBConfig.ChangeFeedCheckpointLocation -> checkpointPath,
      "SamplingRatio" -> "1.0")

    val readConfig = Config(configMap)
    val readConfigUseNextToken = Config(configMap.+((CosmosDBConfig.ChangeFeedUseNextToken, true.toString)))

    val coll = spark.sqlContext.read.cosmosDB(readConfig)

    val documentClient = new DocumentClient(host, key, new ConnectionPolicy(), ConsistencyLevel.Session)
    val collectionLink = s"dbs/$dbName/colls/$collName"

    // VERIFY change feed starting from the beginning
    if (startFromTheBeginning) {
      coll.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect().sortBy(x => x) should
        contain theSameElementsAs (1 to documentCount).toList
    } else {
      coll.count() should equal(0)
    }

    // VERIFY the next change feed batch if nothing happens
    Thread.sleep(3000)
    coll.count() should equal(0)

    val ReadChangeFeedIterations = 4
    // The number of iterations to use next tokens, must be <= ReadChangeFeedIterations
    val UseNextTokenIterations = 3

    (1 to ReadChangeFeedIterations).foreach(iteration => {
      // Create documents with ID ranging from (iteration * documentCount + 1) -> (iteration * documentCount + documentCount)
      (1 to documentCount).foreach(i => {
        val document = new Document()
        document.setId((iteration * documentCount + i).toString)
        document.set(CosmosDBDefaults().PartitionKeyName, document.getId)
        documentClient.createDocument(collectionLink, document, null, false)
      })

      // Update documents with ID ranging from (iteration * document - documentCount / 2) -> iteration * documentCount
      (0 until documentCount / 2).foreach(i => {
        val documentId: String = (iteration * documentCount - i).toString
        val documentLink = s"dbs/$dbName/colls/$collName/docs/$documentId"

        val requestOptions = new RequestOptions()
        requestOptions.setPartitionKey(new PartitionKey(documentId))
        val readDocument = documentClient.readDocument(documentLink, requestOptions).getResource
        readDocument.set("status", "updated")
        documentClient.replaceDocument(documentLink, readDocument, requestOptions)
      })

      // Determine if the result should include changes from the first query or from the last query
      val shouldUseNextToken = useNextToken && iteration > ReadChangeFeedIterations - UseNextTokenIterations
      val gettingFromLastChange = rollingChangeFeed || shouldUseNextToken

      val startIndex = (if (gettingFromLastChange) iteration * documentCount else documentCount) - documentCount / 2 + 1
      val endIndex = iteration * documentCount + documentCount

      val changes = if (shouldUseNextToken)
        spark.sqlContext.read.cosmosDB(readConfigUseNextToken)
      else
        coll

      changes.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect().sortBy(x => x) should
        contain theSameElementsAs (startIndex to endIndex).toList
    })
  }

  it should "support simple incremental view" in withSparkSession() { spark =>
    spark.sparkContext.parallelize((1 to documentCount).map(x => new Document(s"{ id: '$x' }"))).saveToCosmosDB()

    val host = CosmosDBDefaults().CosmosDBEndpoint
    val key = CosmosDBDefaults().CosmosDBKey
    val dbName = CosmosDBDefaults().DatabaseName
    val collName = getTestCollectionName

    val checkpointPath = "./changefeedcheckpoint"

    FileUtils.deleteDirectory(new File(checkpointPath))

    val readConfig = Config(Map("Endpoint" -> host,
      "Masterkey" -> key,
      "Database" -> dbName,
      "Collection" -> collName,
      "IncrementalView" -> "true",
      "ChangeFeedQueryName" -> "incremental view",
      "SamplingRatio" -> "1.0",
      CosmosDBConfig.ChangeFeedCheckpointLocation -> checkpointPath))

    var coll = spark.sqlContext.read.cosmosDB(readConfig)

    coll.count() should equal(documentCount)

    val documentClient = new DocumentClient(host, key, new ConnectionPolicy(), ConsistencyLevel.Session)
    val collectionLink = s"dbs/$dbName/colls/$collName"

    val IncrementalViewReadIterations = 3

    (1 to IncrementalViewReadIterations).foreach(b => {
      (1 to documentCount).foreach(i => {
        val document = new Document()
        document.setId((b * documentCount + i).toString)
        documentClient.createDocument(collectionLink, document, null, false)
      })

      coll = spark.sqlContext.read.cosmosDB(readConfig)

      coll.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect() should
        contain theSameElementsAs (1 to (b * documentCount + documentCount)).toList
    })
  }

  it should "should work with data with a property containing integer and string values" in withSparkContext() { sc =>
    var largeCount = 1001
    sc.parallelize((1 to largeCount).map(x => {
      if (x <= largeCount - 2)
        new Document(s"{pkey: $x, intValue: $x}")
      else if (x == largeCount - 1)
        // document with intValue property with a different type
        new Document(s"{pkey: $x, intValue: 'abc'}")
      else
        // document with missing intValue property
        new Document(s"{pkey: $x}")
    })).saveToCosmosDB()

    val sparkSession = createOrGetDefaultSparkSession(sc)

    val coll = sparkSession.sqlContext.read.cosmosDB()
    coll.createOrReplaceTempView("c")

    var query = "SELECT c.intValue + 1 FROM c"
    var expectedValues = new ListBuffer[Any]
    expectedValues ++= (2 until largeCount)
    expectedValues += null
    expectedValues += null
    var df = sparkSession.sql(query)
    df.count() shouldBe largeCount
    df.rdd.map(x => x.get(0)).collect() should contain theSameElementsAs expectedValues
  }

  it should "send query to target partitions only" in withSparkContext() { sc =>
    sc.parallelize((1 to documentCount).map(x => new Document(s"{pkey: $x}"))).saveToCosmosDB()

    val sparkSession = createOrGetDefaultSparkSession(sc)

    val coll = sparkSession.sqlContext.read.cosmosDB()
    coll.createOrReplaceTempView("c")

    sparkSession.sql("SELECT * FROM c WHERE c.pkey = 1").rdd.getNumPartitions should equal(1)
    sparkSession.sql("SELECT * FROM c WHERE c.pkey IN (1, 2)").rdd.getNumPartitions should equal(2)
    sparkSession.sql("SELECT * FROM c").rdd.getNumPartitions should equal(coll.rdd.getNumPartitions)
  }

  it should "should be easily created from the SQLContext and load from CosmosDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.cosmosDB()

    val df = sparkSession.read.cosmosDB()

    val expectedSchema: StructType = {
      DataTypes.createStructType(Array(
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("_self", DataTypes.StringType, true),
        DataTypes.createStructField("pkey", DataTypes.IntegerType, true),
        DataTypes.createStructField("_ts", DataTypes.IntegerType, true),
        DataTypes.createStructField("_etag", DataTypes.StringType, true),
        DataTypes.createStructField("intString", DataTypes.StringType, true),
        DataTypes.createStructField("_rid", DataTypes.StringType, true),
        DataTypes.createStructField("_attachments", DataTypes.StringType, true)))
    }

    df.schema.fields should contain theSameElementsAs expectedSchema.fields
    expectedSchema.fields should contain theSameElementsAs df.schema.fields
    df.count() should equal(documentCount)
    df.filter(s"pkey = ${documentCount / 2}").map(x => x.getInt(x.fieldIndex("pkey"))).collect() should equal(Array(documentCount / 2))
    df.filter(s"pkey > ${documentCount / 2}").count() should equal(documentCount / 2)
    df.filter($"pkey" > documentCount / 2).count() should equal(documentCount / 2)
    df.filter(s"pkey >= ${documentCount / 2}").count() should equal(documentCount / 2 + 1)
    df.filter(s"pkey > ${documentCount / 4}").filter(s"pkey <= ${documentCount / 2}").count() should equal(documentCount / 4)
    df.filter(s"pkey < ${documentCount / 2}").count() should equal(documentCount / 2 - 1)
    df.filter(s"pkey <= ${documentCount / 2}").count() should equal(documentCount / 2)
    df.filter(s"intString = \'${documentCount / 2}\'").count() should equal(1)
    df.filter($"intString".contains(documentCount / 2)).count() should equal(1)
    df.filter($"intString".startsWith("0")).count() should equal(0)
    df.where($"intString".endsWith("0") && !$"intString".isNotNull).count() should equal(0)
    df.filter($"pkey".isin(documentCount / 2 to documentCount:_*)).rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should
      contain theSameElementsAs (documentCount / 2 to documentCount).toList
    val somePrimeStrings = List("2", "19", "43", "47", "53", "73", "97")
    df.filter($"intString".isin(somePrimeStrings:_*)).rdd.map(x => x.getString(x.fieldIndex("intString"))).collect() should
      contain theSameElementsAs somePrimeStrings
    val somePrimeVals = List(2, 11)
    df.filter($"pkey".isin(somePrimeVals:_*)).rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should
      contain theSameElementsAs somePrimeVals
  }

  it should "should be easily created from the SQLContext and load a lot of documents from CosmosDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    val largeNumberOfDocuments = 1000
    val manyDocuments: IndexedSeq[SimpleDocument] = (1 to largeNumberOfDocuments)
      .map(x => SimpleDocument(x.toString, x, (largeNumberOfDocuments - x + 1).toString))

    // write some documents to CosmosDB and load them back
    sc.parallelize(manyDocuments).toDF().write.cosmosDB()
    var df = sparkSession.read.cosmosDB()
    df.rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should contain theSameElementsAs (1 to largeNumberOfDocuments).toList

    // create an RDD specifying number of slices and load
    sc.parallelize(manyDocuments, 5).toDF().write.mode(SaveMode.Overwrite).cosmosDB()
    df = sparkSession.read.cosmosDB()
    df.rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should contain theSameElementsAs (1 to largeNumberOfDocuments).toList
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.cosmosDB()

    val df = sparkSession.read.cosmosDB[SimpleDocument]()
    val reflectedSchema: StructType = ScalaReflection.schemaFor[SimpleDocument].dataType.asInstanceOf[StructType]

    df.schema.fields should contain theSameElementsAs reflectedSchema.fields
    reflectedSchema.fields should contain theSameElementsAs df.schema.fields
    df.count() should equal(documentCount)
    df.filter(s"pkey > ${documentCount / 2}").count() should equal(documentCount / 2)
  }

  it should "handle selecting out of order columns" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.cosmosDB()

    val df = sparkSession.read.cosmosDB()

    df.select("id", "intString").orderBy("intString").rdd.map(r => (r.get(0), r.get(1))).collect() should
      equal(simpleDocuments.sortBy(_.intString).map(doc => (doc.id, doc.intString)))
  }

  // DataFrame
  "DataFrame" should "be able to round trip schemas containing MapTypes and other types" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    val characterMap = simpleDocuments.map(doc =>
      Row(doc.id,
        Map("platform" -> "Azure CosmosDB", "doc" -> doc.id),
        doc.pkey,
        if (doc.id.hashCode % 2 == 0) true else false,
        Array(doc.id, doc.intString),
        new Date(System.currentTimeMillis()),
        Math.PI,
        Long.MaxValue,
        new Timestamp(System.currentTimeMillis()),
        doc.id.getBytes
      )
    )
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("attributes", MapType(StringType, StringType), nullable = true),
      StructField("pkey", IntegerType, nullable = false),
      StructField("hasEvenHash", BooleanType, nullable = false),
      StructField("arrayId", ArrayType(StringType), nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("doubleNumber", DoubleType, nullable = false),
      StructField("longNumber", LongType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("binary", BinaryType, nullable = false)
    ))
    val df = sparkSession.createDataFrame(sc.parallelize(characterMap), schema)
    df.write.cosmosDB()

    val savedDF = sparkSession.read.schema(schema).cosmosDB()
    savedDF.collectAsList() should contain theSameElementsAs df.collect()
  }

  it should "work with complex json type" in withSparkSession() { spark =>
    val random: Random = new Random
    val maxSchoolCount = 5
    val maxRecordCount = 10
    spark.sparkContext.parallelize((1 to documentCount).map(i => {
      val recordJson = (1 to random.nextInt(maxRecordCount)).mkString("[", ",", "]")
      val schoolsJson = (1 to random.nextInt(maxSchoolCount))
        .map(c => s"{'sname': 'school $c', year: $c, record: $recordJson}")
        .toList
        .mkString("[", ",", "]")
      new Document(s"{id: '$i', pkey: $i, name: 'name $i', schools: $schoolsJson}")
    })).saveToCosmosDB()

    val expectedSchema: StructType = {
      DataTypes.createStructType(Array(
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("_self", DataTypes.StringType, true),
        DataTypes.createStructField("pkey", DataTypes.IntegerType, true),
        DataTypes.createStructField("_ts", DataTypes.IntegerType, true),
        DataTypes.createStructField("_etag", DataTypes.StringType, true),
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("_rid", DataTypes.StringType, true),
        DataTypes.createStructField("schools",
          DataTypes.createArrayType(
            DataTypes.createStructType(Array(
              DataTypes.createStructField("record",
                DataTypes.createArrayType(DataTypes.IntegerType, false),
                true),
              DataTypes.createStructField("sname", DataTypes.StringType, true),
              DataTypes.createStructField("year", DataTypes.IntegerType, true)
            )),
            false),
          true),
        DataTypes.createStructField("_attachments", DataTypes.StringType, true)))
    }

    // Verify that the schema is maintained in a round trip
    var df = spark.read.cosmosDB()
    df.write.mode(SaveMode.Overwrite).cosmosDB()
    df = spark.read.cosmosDB()
    df.schema.fields should contain theSameElementsAs expectedSchema.fields
    expectedSchema.fields should contain theSameElementsAs df.schema.fields
    df.collect().length should equal(documentCount)
  }

  it should "work with nullable struct property" in withSparkSession() { spark =>
    spark.sparkContext.parallelize((1 to 2).map {
      case 1 => new Document("{ \"id101\" : null }")
      case 2 => new Document("{ \"id101\" : { \"c\" : \"A\", \"i\" : \"B\" } }  }")
    }).saveToCosmosDB()

    val df = spark.read.cosmosDB()

    val expectedSchema: StructType = {
      DataTypes.createStructType(Array(
        DataTypes.createStructField("_attachments", DataTypes.StringType, true),
        DataTypes.createStructField("_etag", DataTypes.StringType, true),
        DataTypes.createStructField("_rid", DataTypes.StringType, true),
        DataTypes.createStructField("_self", DataTypes.StringType, true),
        DataTypes.createStructField("_ts", DataTypes.IntegerType, true),
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("id101", DataTypes.createStructType(Array(
          DataTypes.createStructField("c", DataTypes.StringType, true),
          DataTypes.createStructField("i", DataTypes.StringType, true)
        )), true)
      ))
    }

    expectedSchema.fields should contain theSameElementsAs df.schema.fields
    df.schema.fields should contain theSameElementsAs expectedSchema.fields

    // This should not throw
    df.select("id101").count() should equal(2)
    df.filter("id101 is not null").count() should equal(1)
  }

  it should "work with nullable number property" in withSparkSession() { spark =>
    spark.sparkContext.parallelize((1 to 2).map {
      case 1 => new Document("{ \"id101\" : null, \"id102\" : null }")
      case 2 => new Document("{ \"id101\" : 123, \"id102\" : 123.123 }")
    }).saveToCosmosDB()

    val df = spark.read.cosmosDB()

    val expectedSchema: StructType = {
      DataTypes.createStructType(Array(
        DataTypes.createStructField("_attachments", DataTypes.StringType, true),
        DataTypes.createStructField("_etag", DataTypes.StringType, true),
        DataTypes.createStructField("_rid", DataTypes.StringType, true),
        DataTypes.createStructField("_self", DataTypes.StringType, true),
        DataTypes.createStructField("_ts", DataTypes.IntegerType, true),
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("id101", DataTypes.IntegerType, true),
        DataTypes.createStructField("id102", DataTypes.DoubleType, true)
      ))
    }

    expectedSchema.fields should contain theSameElementsAs df.schema.fields
    df.schema.fields should contain theSameElementsAs expectedSchema.fields

    // This should not throw
    df.select("id101").count() should equal(2)
    df.select("id102").count() should equal(2)
    df.filter("id102 is null").count() should equal(1)
    df.filter("id101 is not null").count() should equal(1)
  }

  // Structured stream
  "Structured Stream" should "be able to stream change feed from source collection to sink collection" in withSparkSession() { spark =>
    val host = CosmosDBDefaults().CosmosDBEndpoint
    val key = CosmosDBDefaults().CosmosDBKey
    val databaseName = CosmosDBDefaults().DatabaseName
    val sinkCollection = String.format("CosmosDBSink-%s", System.currentTimeMillis().toString)
    val streamingTimeMs = TimeUnit.SECONDS.toMillis(10)
    val streamingGapMs = TimeUnit.SECONDS.toMillis(10)
    val insertIntervalMs = TimeUnit.SECONDS.toMillis(1) / 2
    // There is a delay from starting the writing to the stream to the first data being written
    val streamingSinkDelayMs = TimeUnit.SECONDS.toMillis(7)
    val insertIterations: Int = ((streamingGapMs * 2 + streamingTimeMs) / insertIntervalMs).toInt

    val cfCheckpointPath = "./changefeedcheckpoint"
    FileUtils.deleteDirectory(new File(cfCheckpointPath))

    var configMap = Config(spark.sparkContext.getConf)
      .asOptions
      .+((CosmosDBConfig.ChangeFeedCheckpointLocation, cfCheckpointPath))

    val databaseLink = s"dbs/$databaseName"
    val sourceCollectionLink = s"$databaseLink/colls/$getTestCollectionName"

    // Create the sink collection
    val documentClient = new DocumentClient(host, key, new ConnectionPolicy(), ConsistencyLevel.Session)
    val sinkCollectionLink = s"$databaseLink/colls/$sinkCollection"
    val documentCollection = new DocumentCollection()
    documentCollection.setId(sinkCollection)
    documentClient.createCollection(databaseLink, documentCollection, null)

    /*
     * SCENARIO 1: STREAM READER TO STREAM WRITER TO SINK COLLECTION
     *
     * Start a thread to insert documents to the source collection.
     * The documents have ID ranging from 1 -> insertIterations.
     *
     * In the middle of that process, create a DataFrame from the change feed of the source collection.
     * After that, create a DataStreamWriter to read the streaming DataFrame to the sink collection.
     *
     * When all documents are written, verify that the sink collection has an acceptable number of documents
     * from the source collection.
     */

    var docIdIndex = 1

    val insertingRunnable = new Runnable() {
      override def run(): Unit = {
        (docIdIndex until docIdIndex + insertIterations).foreach(i => {
          val newDoc = new Document()
          newDoc.setId(s"$i")
          newDoc.set(cosmosDBDefaults.PartitionKeyName, i)
          newDoc.set("content", s"sample content for document with ID $i")
          documentClient.createDocument(sourceCollectionLink, newDoc, null, true)
          logInfo(s"Created document with ID $i")
          TimeUnit.MILLISECONDS.sleep(insertIntervalMs)
        })
        docIdIndex = docIdIndex + insertIterations
      }
    }

    // Start the thread to add new documents
    var insertingThread = new Thread(insertingRunnable)
    insertingThread.start()

    val sourceConfigMap = configMap.
      +((CosmosDBConfig.ChangeFeedQueryName, "Structured Stream unit test"))

    // Start to read the stream
    var streamData = spark.readStream
      .format(classOf[CosmosDBSourceProvider].getName)
      .options(sourceConfigMap)
      .load()

    // Run inserting documents for a few seconds before starting to write to the sink stream
    TimeUnit.MILLISECONDS.sleep(streamingGapMs - insertIntervalMs)

    val sinkConfigMap = configMap.
      -(CosmosDBConfig.Collection).
      +((CosmosDBConfig.Collection, sinkCollection))

    val checkpointPath = "./checkpoint"
    FileUtils.deleteDirectory(new File(checkpointPath))

    // Start to write the stream
    val streamingQueryWriter = streamData.writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .outputMode("append")
      .options(sinkConfigMap)
      .option("checkpointLocation", checkpointPath)

    var streamingQuery = streamingQueryWriter.start()

    TimeUnit.MILLISECONDS.sleep(streamingTimeMs)

    insertingThread.join()

    TimeUnit.MILLISECONDS.sleep(streamingGapMs)

    // Verify the documents have streamed to the new collection
    val df = spark.read.cosmosDB(Config(sinkConfigMap))
    var streamedIdCheckRangeStart = streamingSinkDelayMs / insertIntervalMs
    var streamedIdCheckRangeEnd = insertIterations
    df.rdd.map(row => row.getString(row.fieldIndex("id")).toInt).collect().sortBy(x => x) should
      contain allElementsOf (streamedIdCheckRangeStart to streamedIdCheckRangeEnd).toList

    streamingQuery.stop()

    /*
     * SCENARIO 2: NEW STREAM WRITER
     *
     * Similar to scenario 2, except we create a new DataStreamWriter to continue from the previous checkpoints.
     *
     * Also start a new thread to insert a batch of documents with ID from insertIterations + 1 -> insertIterations * 2.
     *
     * Verify that the sink collection contains the changes spanning from the previous batch to the current batch,
     * without missing any changes in between.
      */

    logInfo("Starting scenario of new stream writer")

    // Start to insert more data
    insertingThread = new Thread(insertingRunnable)
    insertingThread.start()
    TimeUnit.MILLISECONDS.sleep(streamingGapMs)

    // Resume writing to the stream
    streamingQuery = streamData.writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .outputMode("append")
      .options(sinkConfigMap)
      .option("checkpointLocation", checkpointPath)
      .start()

    // Wait for the inserting thread to complete
    insertingThread.join()

    TimeUnit.MILLISECONDS.sleep(streamingGapMs)

    // Verify that the documents have streamed to the new collection
    streamedIdCheckRangeEnd = insertIterations * 2
    df.rdd.map(row => row.getString(row.fieldIndex("id")).toInt).collect().sortBy(x => x) should
      contain allElementsOf (streamedIdCheckRangeStart to streamedIdCheckRangeEnd).toList

    streamingQuery.stop()
    CosmosDBRDDIterator.resetCollectionContinuationTokens()

    /*
     * SCENARIO 3: NEW STREAM READER AND NEW STREAM WRITER
     *
     * Similar to scenario 1, except we create a new StreamReader and a new StreamWriter to simulate node failures.
     *
     * Another batch of changes will be written with ID from insertIterations * 2 + 1 -> insertIterations * 3
     *
     * Verify that the sink collection receives all changes from the first batch without missing any documents in between.
      */

    logInfo("Starting scenario of new stream reader and new stream writer")

    // Start to insert more data
    insertingThread = new Thread(insertingRunnable)
    insertingThread.start()
    TimeUnit.MILLISECONDS.sleep(streamingGapMs)

    // Start to read change feed stream again
    streamData = spark.readStream
      .format(classOf[CosmosDBSourceProvider].getName)
      .options(sourceConfigMap)
      .load()

    // Start to write to the stream again
    streamingQuery = streamData.writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .outputMode("append")
      .options(sinkConfigMap)
      .option("checkpointLocation", checkpointPath)
      .start()

    // Wait for the inserting thread to complete
    insertingThread.join()

    TimeUnit.MILLISECONDS.sleep(streamingGapMs)

    // Verify that the documents have streamed to the new collection
    streamedIdCheckRangeEnd = insertIterations * 3
    df.rdd.map(row => row.getString(row.fieldIndex("id")).toInt).collect().sortBy(x => x) should
      contain allElementsOf (streamedIdCheckRangeStart to streamedIdCheckRangeEnd).toList

    streamingQuery.stop()
    CosmosDBRDDIterator.resetCollectionContinuationTokens()
  }

  it should "work with a slow source" in withSparkSession() { spark =>
    val host = CosmosDBDefaults().CosmosDBEndpoint
    val key = CosmosDBDefaults().CosmosDBKey
    val databaseName = CosmosDBDefaults().DatabaseName
    val partitionKey = cosmosDBDefaults.PartitionKeyName
    val sinkCollection = String.format("CosmosDBSink-%s", System.currentTimeMillis().toString)

    val cfCheckpointPath = "./changefeedcheckpoint"
    FileUtils.deleteDirectory(new File(cfCheckpointPath))

    var configMap = Config(spark.sparkContext.getConf)
      .asOptions
      .+((CosmosDBConfig.ChangeFeedCheckpointLocation, cfCheckpointPath))

    val databaseLink = s"dbs/$databaseName"
    val sourceCollectionLink = s"$databaseLink/colls/$getTestCollectionName"

    // Create the sink collection
    val documentClient = new DocumentClient(host, key, new ConnectionPolicy(), ConsistencyLevel.Session)
    val sinkCollectionLink = s"$databaseLink/colls/$sinkCollection"
    val documentCollection = new DocumentCollection()
    documentCollection.setId(sinkCollection)
    documentClient.createCollection(databaseLink, documentCollection, null)

    // Create some documents in the collection
    // These existing documents are needed to derive the starting schema
    (1 to 100).foreach(i => {
      val doc = new Document()
      doc.setId(i.toString)
      doc.set(partitionKey, s"partitionkey$i")
      documentClient.createDocument(sourceCollectionLink, doc, null, true)
    })

    val sourceConfigMap = configMap.
      +((CosmosDBConfig.ChangeFeedQueryName, "Structured Stream unit test"))

    // Start to read the stream
    var streamData = spark.readStream
      .format(classOf[CosmosDBSourceProvider].getName)
      .options(sourceConfigMap)
      .load()

    val checkpointPath = "./checkpoint"
    FileUtils.deleteDirectory(new File(checkpointPath))

    val sinkConfigMap = configMap.
      -(CosmosDBConfig.Collection).
      +((CosmosDBConfig.Collection, sinkCollection))

    // Start to write the stream
    val streamingQueryWriter = streamData.writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .outputMode("append")
      .options(sinkConfigMap)
      .option("checkpointLocation", checkpointPath)

    var streamingQuery = streamingQueryWriter.start()

    // Let streams ready
    TimeUnit.SECONDS.sleep(20)

    // Create 1 document
    val doc1 = new Document()
    doc1.setId("doc1")
    doc1.set(partitionKey, "partitionkey1")
    documentClient.createDocument(sourceCollectionLink, doc1, null, true)

    // Let the sink idle
    TimeUnit.SECONDS.sleep(20)

    // Create 1 document again
    val doc2 = new Document()
    doc2.setId("doc2")
    doc2.set(partitionKey, "partitionkey2")
    documentClient.createDocument(sourceCollectionLink, doc2, null, true)

    // Wait sometime for the streaming of the second document
    TimeUnit.SECONDS.sleep(20)

    // Verify that all documents make to the sink collection
    val df = spark.read.cosmosDB(Config(sinkConfigMap))
    df.rdd.map(row => row.getString(row.fieldIndex("id"))).collect().sortBy(x => x) should
      contain allElementsOf List(doc1.getId, doc2.getId)

    streamingQuery.stop()
  }
}
