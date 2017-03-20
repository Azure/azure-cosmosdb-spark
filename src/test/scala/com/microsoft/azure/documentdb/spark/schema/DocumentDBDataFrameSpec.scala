package com.microsoft.azure.documentdb.spark.schema

import java.sql.{Date, Timestamp}

import com.microsoft.azure.documentdb.Document
import com.microsoft.azure.documentdb.spark._
import com.microsoft.azure.documentdb.spark.config.{Config, DocumentDBConfig}
import com.microsoft.azure.documentdb.spark.rdd.DocumentDBRDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ListBuffer

case class SimpleDocument(id: String, pkey: Int, intString: String)

class DocumentDBDataFrameSpec extends RequiresDocumentDB {
  val documentCount = 100
  val simpleDocuments: IndexedSeq[SimpleDocument] = (1 to documentCount)
    .map(x => SimpleDocument(x.toString, x, (documentCount - x + 1).toString))

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

  // DataFrameWriter
  "DataFrameWriter" should "be easily created from a DataFrame and save to DocumentDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.documentDB()

    var documentDBRDD: DocumentDBRDD = sc.loadFromDocumentDB()
    documentDBRDD.map(x => x.getInt("pkey")).collect() should contain theSameElementsAs (1 to documentCount).toList
    documentDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs (1 to documentCount).toList

    // Create new documents and overwrite the previous ones
    sc.parallelize((1 to documentCount)
      .map(x => SimpleDocument(x.toString, x, ((documentCount - x + 1) + documentCount).toString)))
      .toDF().write.mode(SaveMode.Overwrite).documentDB()
    documentDBRDD = sc.loadFromDocumentDB()
    var expectedNewValues = (documentCount + 1 to documentCount * 2).toList
    documentDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs expectedNewValues
  }

  it should "take custom writeConfig" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    val config: Config = Config(sc)
    val databaseName: String = config.get(DocumentDBConfig.Database).getOrElse(DocumentDBConfig.Database)
    val collectionName: String = "NewCollection"
    documentDBDefaults.createCollection(databaseName, collectionName)
    var configMap: collection.Map[String, String] = config.asOptions
    configMap = configMap.updated(DocumentDBConfig.Collection, collectionName)
    val newConfig: Config = Config(configMap)

    sc.parallelize(simpleDocuments).toDF().write.documentDB(newConfig)
    var documentDBRDD: DocumentDBRDD = sc.loadFromDocumentDB(newConfig)
    documentDBRDD.map(x => x.getInt("intString")).collect() should contain theSameElementsAs (1 to documentCount).toList

    documentDBDefaults.deleteCollection(databaseName, collectionName)
  }

  // DataFrameReader
  "DataFrameReader" should "should be easily created from SQLContext query" in withSparkContext() { sc =>
    sc.parallelize((1 to documentCount).map(x => new Document(s"{pkey: $x}"))).saveToDocumentDB()

    val sparkSession = createOrGetDefaultSparkSession(sc)

    val coll = sparkSession.sqlContext.read.DocumentDB()
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
    })).saveToDocumentDB()

    val sparkSession = createOrGetDefaultSparkSession(sc)

    val coll = sparkSession.sqlContext.read.DocumentDB()
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

  it should "should be easily created from the SQLContext and load from DocumentDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.documentDB()

    val df = sparkSession.read.DocumentDB()

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
  }

  it should "should be easily created from the SQLContext and load a lot of documents from DocumentDB" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    val largeNumberOfDocuments = 1000
    val manyDocuments: IndexedSeq[SimpleDocument] = (1 to largeNumberOfDocuments)
      .map(x => SimpleDocument(x.toString, x, (largeNumberOfDocuments - x + 1).toString))

    // write some documents to DocumentDB and load them back
    sc.parallelize(manyDocuments).toDF().write.documentDB()
    var df = sparkSession.read.DocumentDB()
    df.rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should contain theSameElementsAs (1 to largeNumberOfDocuments).toList

    // create an RDD specifying number of slices and load
    sc.parallelize(manyDocuments, 5).toDF().write.mode(SaveMode.Overwrite).documentDB()
    df = sparkSession.read.DocumentDB()
    df.rdd.map(x => x.getInt(x.fieldIndex("pkey"))).collect() should contain theSameElementsAs (1 to largeNumberOfDocuments).toList
  }

  it should "be easily created with a provided case class" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.documentDB()

    val df = sparkSession.read.DocumentDB[SimpleDocument]()
    val reflectedSchema: StructType = ScalaReflection.schemaFor[SimpleDocument].dataType.asInstanceOf[StructType]

    df.schema should equal(reflectedSchema)
    df.count() should equal(documentCount)
    df.filter(s"pkey > ${documentCount / 2}").count() should equal(documentCount / 2)
  }

  it should "handle selecting out of order columns" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    import sparkSession.implicits._

    sc.parallelize(simpleDocuments).toDF().write.documentDB()

    val df = sparkSession.read.DocumentDB()

    df.select("id", "intString").orderBy("intString").rdd.map(r => (r.get(0), r.get(1))).collect() should
      equal(simpleDocuments.sortBy(_.intString).map(doc => (doc.id, doc.intString)))
  }

  // DataFrame
  "DataFrame" should "be able to round trip schemas containing MapTypes and other types" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)
    val characterMap = simpleDocuments.map(doc =>
      Row(doc.id,
        Map("platform" -> "Azure DocumentDB", "doc" -> doc.id),
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
    df.write.documentDB()

    val savedDF = sparkSession.read.schema(schema).DocumentDB()
    savedDF.collectAsList() should contain theSameElementsAs df.collect()
  }
}