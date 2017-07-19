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

import java.sql.{Date, Timestamp}

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.{RequiresCosmosDB, _}
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.rdd.CosmosDBRDD
import com.microsoft.azure.documentdb._
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
    val collectionName: String = "NewCollection"
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

    val host = CosmosDBDefaults().EMULATOR_ENDPOINT
    val key = CosmosDBDefaults().EMULATOR_MASTERKEY
    val dbName = CosmosDBDefaults().DATABASE_NAME
    val collName = collectionName

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
    verifyReadChangeFeed(rollingChangeFeed = false, startFromTheBeginning = true, spark)
  }

  it should "read documents rolling change feed" in withSparkSession() { spark =>
    verifyReadChangeFeed(rollingChangeFeed = true, startFromTheBeginning = true, spark)
  }

  it should "read documents change feed not starting from the beginning" in withSparkSession() { spark =>
    verifyReadChangeFeed(rollingChangeFeed = false, startFromTheBeginning = false, spark)
  }

  it should "read documents rolling change feed not starting from the beginning" in withSparkSession() { spark =>
    verifyReadChangeFeed(rollingChangeFeed = true, startFromTheBeginning = false, spark)
  }

  def verifyReadChangeFeed(rollingChangeFeed: Boolean, startFromTheBeginning: Boolean, spark: SparkSession): Unit = {
    spark.sparkContext.parallelize((1 to documentCount).
      map(x => new Document(s"{ id: '$x', ${CosmosDBDefaults().PartitionKeyName}: '$x' }"))).
      saveToCosmosDB()

    val host = CosmosDBDefaults().EMULATOR_ENDPOINT
    val key = CosmosDBDefaults().EMULATOR_MASTERKEY
    val dbName = CosmosDBDefaults().DATABASE_NAME
    val collName = collectionName

    val readConfig = Config(Map("Endpoint" -> host,
      "Masterkey" -> key,
      "Database" -> dbName,
      "Collection" -> collName,
      "ReadChangeFeed" -> "true",
      "ChangeFeedQueryName" -> s"read change feed with RollingChangeFeed=$rollingChangeFeed and StartFromTheBeginning=$startFromTheBeginning",
      "ChangeFeedStartFromTheBeginning" -> startFromTheBeginning.toString,
      "RollingChangeFeed" -> rollingChangeFeed.toString,
      "SamplingRatio" -> "1.0"))

    val coll = spark.sqlContext.read.cosmosDB(readConfig)

    val documentClient = new DocumentClient(host, key, new ConnectionPolicy(), ConsistencyLevel.Session)
    val collectionLink = s"dbs/$dbName/colls/$collName"

    if (startFromTheBeginning) {
      coll.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect().sortBy(x => x) should
        contain theSameElementsAs (1 to documentCount).toList
    } else {
      coll.count() should equal(0)
    }

    Thread.sleep(3000)

    coll.count() should equal(0)

    val ReadChangeFeedIterations = 4

    (1 to ReadChangeFeedIterations).foreach(b => {
      // Create documents
      (1 to documentCount).foreach(i => {
        val document = new Document()
        document.setId((b * documentCount + i).toString)
        document.set(CosmosDBDefaults().PartitionKeyName, document.getId)
        documentClient.createDocument(collectionLink, document, null, false)
      })

      // Update documents
      (0 until documentCount / 2).foreach(i => {
        val documentId: String = (b * documentCount - i).toString
        val documentLink = s"dbs/$dbName/colls/$collName/docs/$documentId"

        val requestOptions = new RequestOptions()
        requestOptions.setPartitionKey(new PartitionKey(documentId))
        val readDocument = documentClient.readDocument(documentLink, requestOptions).getResource
        readDocument.set("status", "updated")
        documentClient.replaceDocument(documentLink, readDocument, requestOptions)
      })

      val startIndex = (if (rollingChangeFeed) b * documentCount else documentCount) - documentCount / 2 + 1
      val endIndex = b * documentCount + documentCount

      coll.rdd.map(x => x.getString(x.fieldIndex("id")).toInt).collect().sortBy(x => x) should
        contain theSameElementsAs (startIndex to endIndex).toList
    })
  }

  it should "support simple incremental view" in withSparkSession() { spark =>
    spark.sparkContext.parallelize((1 to documentCount).map(x => new Document(s"{ id: '$x' }"))).saveToCosmosDB()

    val host = CosmosDBDefaults().EMULATOR_ENDPOINT
    val key = CosmosDBDefaults().EMULATOR_MASTERKEY
    val dbName = CosmosDBDefaults().DATABASE_NAME
    val collName = collectionName

    val readConfig = Config(Map("Endpoint" -> host,
      "Masterkey" -> key,
      "Database" -> dbName,
      "Collection" -> collName,
      "IncrementalView" -> "true",
      "ChangeFeedQueryName" -> "incremental view",
      "SamplingRatio" -> "1.0"))

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

    df.schema should equal(expectedSchema)
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

    df.schema should equal(reflectedSchema)
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

  it should "work with complex json type" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    val random: Random = new Random
    val maxSchoolCount = 5
    val maxRecordCount = 10
    sc.parallelize((1 to documentCount).map(x => {
      val recordJson = (1 to random.nextInt(maxRecordCount)).mkString("[", ",", "]")
      val schoolsJson = (1 to random.nextInt(maxSchoolCount))
        .map(c => s"{'sname': 'school $c', year: $c, record: $recordJson}")
        .toList
        .mkString("[", ",", "]")
      new Document(s"{pkey: $x, name: 'name $x', schools: $schoolsJson}")
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

    val df = sparkSession.read.cosmosDB()
    df.schema should equal(expectedSchema)
    df.collect().length should equal(documentCount)
  }
}