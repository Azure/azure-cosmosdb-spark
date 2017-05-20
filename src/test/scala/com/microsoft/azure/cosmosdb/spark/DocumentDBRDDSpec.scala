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

import com.microsoft.azure.cosmosdb.spark.rdd.DocumentDBRDD
import com.microsoft.azure.documentdb.Document
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.immutable.IndexedSeq

class SimpleRDDDocument() extends Document {
  def id: String = getString("id")
  def id_=(value: String): Unit = set("id", value)
  def pkey: Int = getInt("pkey")
  def pkey_= (value: Int): Unit = set("pkey", value)
  def intString: String = getString("intString")
  def intString_= (value: String): Unit = set("intString", value)
}

class DocumentDBRDDSpec extends RequiresDocumentDB {
  val documentCount = 100
  val simpleDocuments: IndexedSeq[SimpleRDDDocument] = (1 to documentCount)
    .map(x => {
      var newDocument = new SimpleRDDDocument()
      newDocument.id_=(x.toString)
      newDocument.pkey_=(x)
      newDocument.intString_=((documentCount - x + 1).toString)
      newDocument
    })

  val expectedSchema: StructType = {
    DataTypes.createStructType(Array(
      DataTypes.createStructField("_etag", DataTypes.StringType, true),
      DataTypes.createStructField("_rid", DataTypes.StringType, true),
      DataTypes.createStructField("_attachments", DataTypes.StringType, true),
      DataTypes.createStructField("intString", DataTypes.StringType, true),
      DataTypes.createStructField("id", DataTypes.StringType, true),
      DataTypes.createStructField("_self", DataTypes.StringType, true),
      DataTypes.createStructField("pkey", DataTypes.IntegerType, true),
      DataTypes.createStructField("_ts", DataTypes.IntegerType, true)))
  }

  "DocumentDBRDD" should "be easily created from the SparkContext" in withSparkContext() { sc =>
    sc.parallelize(simpleDocuments, 2).saveToDocumentDB()
    val documentDBRDD: DocumentDBRDD = sc.loadFromDocumentDB()

    documentDBRDD.count() shouldBe documentCount
    documentDBRDD.map(x => x.getInt("pkey")).collect() should contain theSameElementsAs (1 to documentCount).toList
  }

  it should "be easy to save to all DocumentDB partitions" in withSparkContext() { sc =>
    sc.parallelize(simpleDocuments).saveToDocumentDB()

    val documentDBRDD: DocumentDBRDD = DocumentDBSpark.builder().sparkContext(sc).build().toRDD
    val partitionCount = documentDBRDD.getNumPartitions

    // verify the documents distribute among the partitions withint a margin of distributionMargin percent
    val distributionMargin = 10.0 / 100
    val idealDocsPerPartition = documentCount / partitionCount
    var docsDistribution = documentDBRDD.mapPartitions(iter => Array(iter.size).iterator).collect()
    docsDistribution.foreach(count => assert(Math.abs(count * 1.0 / idealDocsPerPartition - 1) <= distributionMargin))
  }

  it should "be able to handle empty collections" in withSparkContext() { sc =>
    sc.loadFromDocumentDB().count() shouldBe 0
  }

  it should "be able to create a DataFrame by inferring the schema" in withSparkContext() { sc =>
    val sparkSession = createOrGetDefaultSparkSession(sc)

    sc.parallelize(simpleDocuments).saveToDocumentDB()

    val dataFrame: DataFrame = sc.loadFromDocumentDB().toDF()
    dataFrame.schema should equal(expectedSchema)
    dataFrame.count() should equal(documentCount)
  }
}
