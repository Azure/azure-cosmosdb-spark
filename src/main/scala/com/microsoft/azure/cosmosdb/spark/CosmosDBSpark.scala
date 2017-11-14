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

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.{CosmosDBRDD, _}
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import rx.Observable
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkimport.bulkupdate.{BulkUpdateResponse, UpdateItem}
import com.microsoft.azure.documentdb.bulkimport.{BulkImportResponse, DocumentBulkImporter}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * The CosmosDBSpark allow fast creation of RDDs, DataFrames or Datasets from CosmosDBSpark.
  *
  * @since 1.0
  */
object CosmosDBSpark extends LoggingTrait {

  /**
   * The default source string for creating DataFrames from CosmosDB
   */
  val defaultSource = classOf[DefaultSource].getCanonicalName

  /**
    * For verfication purpose
    */
  var lastUpsertSetting: Option[Boolean] = _
  var lastWritingBatchSize: Option[Int] = _

  val random = new Random(System.nanoTime())

  /**
    * Create a builder for configuring the [[CosmosDBSpark]]
    *
    * @return a CosmosDBSession Builder
    */
  def builder(): Builder = new Builder

  /**
    * Load data from CosmosDB
    *
    * @param sc the Spark context containing the CosmosDB connection configuration
    * @return a CosmosDBRDD
    */
  def load(sc: SparkContext): CosmosDBRDD = load(sc, Config(sc))

  /**
    * Load data from CosmosDB
    *
    * @param sc the Spark context containing the CosmosDB connection configuration
    * @return a CosmosDBRDD
    */
  def load(sc: SparkContext, readConfig: Config): CosmosDBRDD =
    builder().sparkContext(sc).config(readConfig).build().toRDD

  /**
    * Load data from CosmosDB
    *
    * @param sparkSession the SparkSession containing the CosmosDB connection configuration
    * @tparam D The optional class defining the schema for the data
    * @return a CosmosDBRDD
    */
  def load[D <: Product : TypeTag](sparkSession: SparkSession): DataFrame =
    load[D](sparkSession, Config(sparkSession))

  /**
    * Load data from CosmosDB
    *
    * @param sparkSession the SparkSession containing the CosmosDB connection configuration
    * @tparam D The optional class defining the schema for the data
    * @return a CosmosDBRDD
    */
  def load[D <: Product : TypeTag](sparkSession: SparkSession, readConfig: Config): DataFrame =
    builder().sparkSession(sparkSession).config(readConfig).build().toDF[D]()

  /**
    * Load data from CosmosDB
    *
    * @param sparkSession the SparkSession containing the CosmosDB connection configuration
    * @param clazz        the class of the data contained in the RDD
    * @tparam D The bean class defining the schema for the data
    * @return a CosmosDBRDD
    */
  def load[D](sparkSession: SparkSession, readConfig: Config, clazz: Class[D]): Dataset[D] =
    builder().sparkSession(sparkSession).config(readConfig).build().toDS(clazz)

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database and collection information
    * Requires a codec for the data type
    *
    * @param rdd the RDD data to save to CosmosDB
    * @tparam D the type of the data in the RDD
    */
  def save[D: ClassTag](rdd: RDD[D]): Unit = save(rdd, Config(rdd.sparkContext))

  /**
    * Save data to CosmosDB
    *
    * @param rdd         the RDD data to save to CosmosDB
    * @param writeConfig the writeConfig
    * @tparam D the type of the data in the RDD
    */
  def save[D: ClassTag](rdd: RDD[D], writeConfig: Config): Unit = {
    var numPartitions = 0
    try {
      numPartitions = rdd.getNumPartitions
    } catch {
      case _: Throwable => // no op
    }

    // Check if we're writing ADLPartition
    var isWritingAdlPartition = false
    var adlPartitionMap = mutable.Map[Int, ADLFilePartition]()
    try {
      // The .partitons call can throw nulref for non-parallel RDD
      val partitions = rdd.partitions
      isWritingAdlPartition = partitions.length > 0 && partitions(0).isInstanceOf[ADLFilePartition]
      if (isWritingAdlPartition) {
        partitions.foreach(p => {
          adlPartitionMap = adlPartitionMap + (p.index -> p.asInstanceOf[ADLFilePartition])
        })
      }
    } catch {
      case _: Throwable => // no op
    }

    val hadoopConfig = HdfsUtils.getConfigurationMap(rdd.sparkContext.hadoopConfiguration).toMap

    val mapRdd = rdd.mapPartitionsWithIndex((partitionId, iter) =>
      if (isWritingAdlPartition) {
        val adlPartition = adlPartitionMap(partitionId)
        saveAdlPartition(iter, writeConfig, numPartitions, adlPartition.adlFilePath, hadoopConfig)
      }
      else
        savePartition(iter, writeConfig, numPartitions), preservesPartitioning = true)
    mapRdd.collect()

//    // All tasks have been completed, clean up the file checkpoints
//    val adlCheckpointPath = writeConfig.get[String](CosmosDBConfig.adlFileCheckpointPath)
//    if (adlCheckpointPath.isDefined) {
//      val hdfsUtils = new HdfsUtils(hadoopConfig)
//      ADLConnection.cleanUpProgress(hdfsUtils, adlCheckpointPath.get)
//    }
  }

  private def bulkUpdate[D: ClassTag](iter: Iterator[D],
                                      connection: CosmosDBConnection,
                                      writingBatchSize: Integer)(implicit ev: ClassTag[D]): Unit = {
    val importer: DocumentBulkImporter = connection.documentBulkImporter
    val updateItems = new java.util.ArrayList[UpdateItem](writingBatchSize)
    val updatePatchItems = new java.util.ArrayList[Document](writingBatchSize)
    var bulkImportResponse: BulkUpdateResponse = null
    iter.foreach(item => {
      item match {
        case updateItem: UpdateItem =>
          updateItems.add(item.asInstanceOf[UpdateItem])
        case doc: Document =>
          updatePatchItems.add(doc)
        case row: Row =>
          updatePatchItems.add(new Document(CosmosDBRowConverter.rowToJSONObject(row).toString()))
        case _ => throw new Exception("Unsupported update item types")
      }
      if (updateItems.size() >= writingBatchSize) {
        bulkImportResponse = importer.updateAll(updateItems)
        updateItems.clear()
      }
      if (updatePatchItems.size() >= writingBatchSize) {
        bulkImportResponse = importer.updateAllWithPatch(updatePatchItems)
        updatePatchItems.clear()
      }
    })
    if (updateItems.size() > 0) {
      bulkImportResponse = importer.updateAll(updateItems)
    }
    if (updatePatchItems.size() > 0) {
      bulkImportResponse = importer.updateAllWithPatch(updatePatchItems)
    }
  }

  private def bulkImport[D: ClassTag](iter: Iterator[D],
                                      connection: CosmosDBConnection,
                                      writingBatchSize: Integer,
                                      rootPropertyToSave: Option[String],
                                      upsert: Boolean): Unit = {
    val importer: DocumentBulkImporter = connection.documentBulkImporter
    val documents = new java.util.ArrayList[String](writingBatchSize)
    var bulkImportResponse: BulkImportResponse = null
    iter.foreach(item => {
      val document: Document = item match {
        case doc: Document => doc
        case row: Row =>
          if (rootPropertyToSave.isDefined) {
            new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
          } else {
            new Document(CosmosDBRowConverter.rowToJSONObject(row).toString())
          }
        case any => new Document(any.toString)
      }
      if (document.getId == null) {
        document.setId(UUID.randomUUID().toString)
      }
      documents.add(document.toJson())
      if (documents.size() >= writingBatchSize) {
        bulkImportResponse = importer.importAll(documents, upsert)
        documents.clear()
      }
    })
    if (documents.size() > 0) {
      bulkImportResponse = importer.importAll(documents, upsert)
    }
  }

  private def importWithRxJava[D: ClassTag](iter: Iterator[D],
                                            connection: CosmosDBConnection,
                                            writingBatchSize: Integer,
                                            writingBatchDelayMs: Long,
                                            rootPropertyToSave: Option[String],
                                            upsert: Boolean): Unit = {
    var observables = new java.util.ArrayList[Observable[ResourceResponse[Document]]](writingBatchSize)
    var createDocumentObs: Observable[ResourceResponse[Document]] = null
    var batchSize = 0
    iter.foreach(item => {
      val document: Document = item match {
        case doc: Document => doc
        case row: Row =>
          if (rootPropertyToSave.isDefined) {
            new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
          } else {
            new Document(CosmosDBRowConverter.rowToJSONObject(row).toString())
          }
        case any => new Document(any.toString)
      }
      logDebug(s"Inserting document $document")
      if (upsert)
        createDocumentObs = connection.upsertDocument(document, null)
      else
        createDocumentObs = connection.createDocument(document, null)
      observables.add(createDocumentObs)
      batchSize = batchSize + 1
      if (batchSize % writingBatchSize == 0) {
        Observable.merge(observables).toBlocking.last()
        if (writingBatchDelayMs > 0) {
          TimeUnit.MILLISECONDS.sleep(writingBatchDelayMs)
        }
        observables.clear()
        batchSize = 0
      }
    })
    if (!observables.isEmpty) {
      Observable.merge(observables).toBlocking.last()
    }
  }

  private def saveAdlPartition[D: ClassTag](iter: Iterator[D],
                                           config: Config,
                                           partitionCount: Int,
                                            adlFilePath: String,
                                            hadoopConfig: Map[String, String]): Iterator[D] = {
    val connection = new CosmosDBConnection(config)
    val iterator = savePartition(connection, iter, config, partitionCount)

    // Mark the adlFile on this partition as processed
    // Todo: refactor this part
    val adlCheckpointPath = config.get[String](CosmosDBConfig.adlFileCheckpointPath)
    if (adlCheckpointPath.isDefined) {
      val hdfsUtils = new HdfsUtils(hadoopConfig)
      ADLConnection.markAdlFileProcessed(hdfsUtils, adlCheckpointPath.get, adlFilePath)
    } else {
      val aldFileStoreCollection = config.get[String](CosmosDBConfig.adlCosmosDBFilestoreCollection)
      if (aldFileStoreCollection.isDefined) {
        val dbName = config.get[String](CosmosDBConfig.Database).get
        val collectionLink = s"/dbs/$dbName/colls/${aldFileStoreCollection.get}"
        ADLConnection.markAdlFileStatus(connection, collectionLink, adlFilePath, isInProgress = false, isComplete = true)
      }
    }

    iterator
  }

  private def savePartition[D: ClassTag](iter: Iterator[D],
                                         config: Config,
                                         partitionCount: Int): Iterator[D] = {
    val connection = new CosmosDBConnection(config)
    savePartition(connection, iter, config, partitionCount)
  }

  private def savePartition[D: ClassTag](connection: CosmosDBConnection,
                                          iter: Iterator[D],
                                          config: Config,
                                          partitionCount: Int): Iterator[D] = {
    val connection = new CosmosDBConnection(config)
    val upsert: Boolean = config
      .getOrElse(CosmosDBConfig.Upsert, String.valueOf(CosmosDBConfig.DefaultUpsert))
      .toBoolean
    val writingBatchSize = config
      .getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize))
      .toInt
    val writingBatchDelayMs = config
      .getOrElse(CosmosDBConfig.WritingBatchDelayMs, String.valueOf(CosmosDBConfig.DefaultWritingBatchDelayMs))
      .toInt
    val rootPropertyToSave = config
      .get[String](CosmosDBConfig.RootPropertyToSave)
    val isBulkUpdating = config.get[String](CosmosDBConfig.BulkUpdate).
      getOrElse(CosmosDBConfig.DefaultBulkUpdate.toString).
      toBoolean
    val isBulkImporting = config.get[String](CosmosDBConfig.BulkImport).
      getOrElse(CosmosDBConfig.DefaultBulkImport.toString).
      toBoolean
    val clientInitDelay = config.get[String](CosmosDBConfig.ClientInitDelay).
      getOrElse(CosmosDBConfig.DefaultClientInitDelay.toString).
      toInt

    // Delay the start as the number of tasks grow to avoid throttling at initialization
    val maxDelaySec: Int = (partitionCount / clientInitDelay) + (if (partitionCount % clientInitDelay > 0) 1 else 0)
    TimeUnit.SECONDS.sleep(random.nextInt(maxDelaySec))

    CosmosDBSpark.lastUpsertSetting = Some(upsert)
    CosmosDBSpark.lastWritingBatchSize = Some(writingBatchSize)

    if (iter.nonEmpty) {
      if (isBulkUpdating) {
        logDebug(s"Writing partition with bulk update")
        bulkUpdate(iter, connection, writingBatchSize)
      } else if (isBulkImporting) {
        logDebug(s"Writing partition with bulk import")
        bulkImport(iter, connection, writingBatchSize, rootPropertyToSave, upsert)
      } else {
        logDebug(s"Writing partition with rxjava")
        importWithRxJava(iter, connection, writingBatchSize, writingBatchDelayMs, rootPropertyToSave, upsert)
      }
    }
    new ListBuffer[D]().iterator
  }

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database and collection information
    *
    * '''Note:''' If the dataFrame contains an `_id` field the data will upserted and replace any existing documents in the collection.
    *
    * @param dataset the dataset to save to CosmosDB
    * @tparam D
    * @since 1.1.0
    */
  def save[D: ClassTag](dataset: Dataset[D]): Unit = save(dataset, Config(dataset.sparkSession.sparkContext.getConf))

  /**
    * Save data to CosmosDB
    *
    * '''Note:''' If the dataFrame contains an `_id` field the data will upserted and replace any existing documents in the collection.
    *
    * @param dataset     the dataset to save to CosmosDB
    * @param writeConfig the writeConfig
    * @tparam D
    * @since 1.1.0
    */
  def save[D: ClassTag](dataset: Dataset[D], writeConfig: Config): Unit = {
    var numPartitions = 0
    try {
      numPartitions = dataset.rdd.getNumPartitions
    } catch {
      case _: Throwable => // no op
    }
    dataset.foreachPartition(iter => savePartition(iter, writeConfig, numPartitions))
  }

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database and collection information
    *
    * @param dataFrameWriter the DataFrameWriter save to CosmosDB
    */
  def save(dataFrameWriter: DataFrameWriter[_]): Unit = dataFrameWriter.format(defaultSource).save()

  /**
    * Save data to CosmosDB
    *
    * @param dataFrameWriter the DataFrameWriter save to CosmosDB
    * @param writeConfig     the writeConfig
    */
  def save(dataFrameWriter: DataFrameWriter[_], writeConfig: Config): Unit =
    dataFrameWriter.format(defaultSource).options(writeConfig.asOptions).save()

  /**
   * Creates a DataFrameReader with `CosmosDB` as the source
   *
   * @param sparkSession the SparkSession
   * @return the DataFrameReader
   */
  def read(sparkSession: SparkSession): DataFrameReader = sparkSession.read.format(defaultSource)

  /**
   * Creates a DataFrameWriter with the `CosmosDB` underlying output data source.
   *
   * @param dataFrame the DataFrame to convert into a DataFrameWriter
   * @return the DataFrameWriter
   */
  def write(dataFrame: DataFrame): DataFrameWriter[Row] = dataFrame.write.format(defaultSource)

  /**
    * Builder for configuring and creating a [[CosmosDBSpark]]
    *
    * It requires a `SparkSession` or the `SparkContext`
    */
  class Builder {
    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[CosmosDBConnection] = None
    private var config: Option[Config] = None
    private var options: collection.Map[String, String] = Map()

    def build(): CosmosDBSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get
      val readConf = config.isDefined match {
        case true => Config(options, config)
        case false => Config(session.sparkContext.getConf, options)
      }

      new CosmosDBSpark(session, readConf)
    }

    /**
      * Sets the SparkSession from the sparkContext
      *
      * @param sparkSession for the RDD
      */
    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Option(sparkSession)
      this
    }

    /**
      * Sets the SparkSession from the sparkContext
      *
      * @param sparkContext for the RDD
      */
    def sparkContext(sparkContext: SparkContext): Builder = {
      this.sparkSession = Option(SparkSession.builder().config(sparkContext.getConf).getOrCreate())
      this
    }

    /**
      * Sets the SparkSession from the javaSparkContext
      *
      * @param javaSparkContext for the RDD
      */
    def javaSparkContext(javaSparkContext: JavaSparkContext): Builder = sparkContext(javaSparkContext.sc)

    /**
      * Append a configuration option
      *
      * These options can be used to configure all aspects of how to connect to CosmosDB
      *
      * @param key   the configuration key
      * @param value the configuration value
      */
    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    /**
      * Set configuration options
      *
      * These options can configure all aspects of how to connect to CosmosDB
      *
      * @param options the configuration options
      */
    def options(options: collection.Map[String, String]): Builder = {
      this.options = options
      this
    }

    /**
      * Set configuration options
      *
      * These options can configure all aspects of how to connect to CosmosDB
      *
      * @param options the configuration options
      */
    def options(options: java.util.Map[String, String]): Builder = {
      this.options = options.asScala
      this
    }

    /**
      * Sets the [[com.microsoft.azure.cosmosdb.spark.config.Config]] to use
      *
      * @param readConfig the readConfig
      */
    def config(config: Config): Builder = {
      this.config = Option(config)
      this
    }
  }

  /*
   * Java API helpers
   */

  /**
    * Load data from CosmosDB
    *
    * @param jsc the Spark context containing the CosmosDB connection configuration
    * @return a CosmosDBRDD
    */
  def load(jsc: JavaSparkContext): JavaCosmosDBRDD = builder().javaSparkContext(jsc).build().toJavaRDD()

  /**
    * Load data from CosmosDB
    *
    * @param jsc the Spark context containing the CosmosDB connection configuration
    * @return a CosmosDBRDD
    */
  def load(jsc: JavaSparkContext, readConfig: Config): JavaCosmosDBRDD =
    builder().javaSparkContext(jsc).config(readConfig).build().toJavaRDD()

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database and collection information
    *
    * @param javaRDD the RDD data to save to CosmosDB
    * @return the javaRDD
    */
  def save(javaRDD: JavaRDD[Document]): Unit = save(javaRDD, classOf[Document])

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database and collection information
    * Requires a codec for the data type
    *
    * @param javaRDD the RDD data to save to CosmosDB
    * @param clazz   the class of the data contained in the RDD
    * @tparam D the type of the data in the RDD
    * @return the javaRDD
    */
  def save[D](javaRDD: JavaRDD[D], clazz: Class[D]): Unit = {
    implicit def ct: ClassTag[D] = ClassTag(clazz)

    save[D](javaRDD.rdd)
  }

  /**
    * Save data to CosmosDB
    *
    * Uses the `SparkConf` for the database information
    *
    * @param javaRDD     the RDD data to save to CosmosDB
    * @param writeConfig the [[com.microsoft.azure.cosmosdb.spark.config.Config]]
    * @return the javaRDD
    */
  def save(javaRDD: JavaRDD[Document], writeConfig: Config): Unit =
    save(javaRDD, writeConfig, classOf[Document])

  /**
    * Save data to CosmosDB
    *
    * Uses the `writeConfig` for the database information
    * Requires a codec for the data type
    *
    * @param javaRDD     the RDD data to save to CosmosDB
    * @param writeConfig the [[com.microsoft.azure.cosmosdb.spark.config.Config]]
    * @param clazz       the class of the data contained in the RDD
    * @tparam D the type of the data in the RDD
    * @return the javaRDD
    */
  def save[D](javaRDD: JavaRDD[D], writeConfig: Config, clazz: Class[D]): Unit = {
    implicit def ct: ClassTag[D] = ClassTag(clazz)

    save[D](javaRDD.rdd, writeConfig)
  }

}

/**
  * The CosmosDBSpark class
  *
  * '''Note:''' Creation of the class should be via [[CosmosDBSpark$.builder]].
  *
  * @since 1.0
  */
case class CosmosDBSpark(sparkSession: SparkSession, readConfig: Config) {

  private def rdd: CosmosDBRDD =
    new CosmosDBRDD(sparkSession, readConfig)

  /**
    * Creates a `RDD` for the collection
    *
    * @tparam D the datatype for the collection
    * @return a CosmosDBRDD[D]
    */
  def toRDD: CosmosDBRDD = rdd

  /**
    * Creates a `JavaRDD` for the collection
    *
    * @return a JavaCosmosDBRDD
    */
  def toJavaRDD(): JavaCosmosDBRDD = rdd.toJavaRDD()

  /**
    * Creates a `DataFrame` based on the schema derived from the optional type.
    *
    * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
    * The rdd must contain an `_id` for CosmosDB versions < 3.2.
    *
    * @tparam T The optional type of the data from CosmosDB, if not provided the schema will be inferred from the collection
    * @return a DataFrame
    */

  def toDF[T <: Product : TypeTag](): DataFrame = {
    val schema: StructType = InferSchema.reflectSchema[T]() match {
      case Some(reflectedSchema) => reflectedSchema
      case None => InferSchema(rdd)
    }
    toDF(schema)
  }

  /**
    * Creates a `DataFrame` based on the schema derived from the bean class.
    *
    * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as computations will be more efficient.
    *
    * @param beanClass encapsulating the data from CosmosDB
    * @tparam T The bean class type to shape the data from CosmosDB into
    * @return a DataFrame
    */
  def toDF[T](beanClass: Class[T]): DataFrame = toDF(InferSchema.reflectSchema[T](beanClass))

  /**
    * Creates a `DataFrame` based on the provided schema.
    *
    * @param schema the schema representing the DataFrame.
    * @return a DataFrame.
    */
  def toDF(schema: StructType): DataFrame = {
    val rowRDD = CosmosDBRowConverter.asRow(schema, rdd)
    sparkSession.createDataFrame(rowRDD, schema)
  }

  /**
    * Creates a `Dataset` from the collection strongly typed to the provided case class.
    *
    * @tparam T The type of the data from CosmosDB
    * @return
    */
  def toDS[T <: Product : TypeTag](): Dataset[T] = {
    val dataFrame: DataFrame = toDF[T]()
    import dataFrame.sqlContext.implicits._
    dataFrame.as[T]
  }

  /**
    * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
    *
    * @tparam T The type of the data from CosmosDB
    * @return
    */
  def toDS[T](beanClass: Class[T]): Dataset[T] = toDF[T](beanClass).as(Encoders.bean(beanClass))

}

