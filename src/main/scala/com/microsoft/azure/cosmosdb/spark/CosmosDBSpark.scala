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

import java.io.PrintWriter
import java.io.StringWriter
import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.{CosmosDBRDD, _}
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import rx.Observable
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.bulkexecutor.{BulkImportResponse, BulkUpdateResponse, DocumentBulkExecutor, UpdateItem}
import org.apache.spark.{Partition, SparkContext}
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
import scala.collection.JavaConversions._

/**
  * The CosmosDBSpark allow fast creation of RDDs, DataFrames or Datasets from CosmosDBSpark.
  *
  * @since 1.0
  */
object CosmosDBSpark extends CosmosDBLoggingTrait {

  /**
   * The default source string for creating DataFrames from CosmosDB
   */
  val defaultSource: String = classOf[DefaultSource].getCanonicalName

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
    val hadoopConfig = HdfsUtils.getConfigurationMap(rdd.sparkContext.hadoopConfiguration)
    var rddNumPartitions = 0
    try {
      numPartitions = rdd.getNumPartitions
      rddNumPartitions = numPartitions
    } catch {
      case _: Throwable => // no op
    }

    val maxMiniBatchImportSizeKB: Int = writeConfig
      .get[String](CosmosDBConfig.MaxMiniBatchImportSizeKB)
      .getOrElse(CosmosDBConfig.DefaultMaxMiniBatchImportSizeKB.toString)
      .toInt

    val writeThroughputBudget : Option[Int] = writeConfig
      .get[String](CosmosDBConfig.WriteThroughputBudget)
      .map(_.toInt)

    var writeThroughputBudgetPerCosmosPartition: Option[Int] = None
    var baseMaxMiniBatchImportSizeKB: Int = maxMiniBatchImportSizeKB

    // if writeThroughputBudget is provided in config, derive baseMaxMiniBatchImportSize based on #sparkPartitions & baseMiniBatchRUConsumption value
    if (writeThroughputBudget.exists(_ > 0)) {
      val baseMiniBatchRUConsumption: Int = writeConfig
        .get[String](CosmosDBConfig.BaseMiniBatchRUConsumption)
        .getOrElse(CosmosDBConfig.DefaultBaseMiniBatchRUConsumption.toString)
        .toInt

      val maxIngestionTaskParallelism: Option[Int] = writeConfig
        .get[String](CosmosDBConfig.MaxIngestionTaskParallelism)
        .map(_.toInt)

      // If the maxIngestionTaskParallelism was provided, use it for baseMaxMiniBatchImportSize derivation.
      // If the df has 100 spark partitions and the spark cluster has only 32 cores, the max task parallelism is 32.
      // In this case, users can set maxIngestionTaskParallelism to 32 and will help with the RU consumption based on writeThroughputBudget.
      if (maxIngestionTaskParallelism.exists(_ > 0)) numPartitions = maxIngestionTaskParallelism.get

      val cosmosPartitionsCount = CosmosDBConnection(writeConfig, hadoopConfig).getAllPartitions.length
      // writeThroughputBudget per cosmos db physical partition
      writeThroughputBudgetPerCosmosPartition = Some((writeThroughputBudget.get / cosmosPartitionsCount).ceil.toInt)
      val baseMiniBatchSizeAdjustmentFactor: Double = (baseMiniBatchRUConsumption.toDouble * numPartitions) / writeThroughputBudgetPerCosmosPartition.get
      if (maxMiniBatchImportSizeKB >= baseMiniBatchSizeAdjustmentFactor) {
        baseMaxMiniBatchImportSizeKB = (maxMiniBatchImportSizeKB / baseMiniBatchSizeAdjustmentFactor).ceil.toInt
      } else {
        // In the rare case of very high #sparkPartitions and very low writeThroughputBudget, derived baseMaxMiniBatchImportSize could be < 1.
        // In this case, the #sparkPartitions needs to be adjusted to limit the RU consumption with the provided writeThroughputBudget
        baseMaxMiniBatchImportSizeKB = 1
        numPartitions = ((maxMiniBatchImportSizeKB * writeThroughputBudgetPerCosmosPartition.get) / baseMiniBatchRUConsumption).ceil.toInt
      }
    }

    val mapRdd = if (numPartitions < rddNumPartitions && numPartitions > 0) {
      rdd.coalesce(numPartitions).mapPartitions(savePartition(_, writeConfig, hadoopConfig, numPartitions,
        baseMaxMiniBatchImportSizeKB * 1024, writeThroughputBudgetPerCosmosPartition), preservesPartitioning = true)
    } else {
      rdd.mapPartitions(savePartition(_, writeConfig, hadoopConfig, numPartitions,
        baseMaxMiniBatchImportSizeKB * 1024, writeThroughputBudgetPerCosmosPartition), preservesPartitioning = true)
    }

    mapRdd.collect()
  }

  private def bulkUpdate[D: ClassTag](iter: Iterator[D],
                                      connection: CosmosDBConnection,
                                      writingBatchSize: Int)(implicit ev: ClassTag[D]): Unit = {
    // Initialize BulkExecutor
    val updater: DocumentBulkExecutor = connection.getDocumentBulkImporter

    // Set retry options to 0 to pass control to BulkExecutor
    // connection.setZeroClientRetryPolicy
    val updateItems = new java.util.ArrayList[UpdateItem](writingBatchSize)
    val updatePatchItems = new java.util.ArrayList[Document](writingBatchSize)
    val cosmosDBRowConverter = new CosmosDBRowConverter(SerializationConfig.fromConfig(connection.config))

    var bulkUpdateResponse: BulkUpdateResponse = null
    iter.foreach(item => {
      item match {
        case updateItem: UpdateItem =>
          updateItems.add(item.asInstanceOf[UpdateItem])
        case doc: Document =>
          updatePatchItems.add(doc)
        case row: Row =>
          updatePatchItems.add(new Document(cosmosDBRowConverter.rowToJSONObject(row).toString()))
        case _ => throw new Exception("Unsupported update item types")
      }
      if (updateItems.size() >= writingBatchSize) {
        bulkUpdateResponse = updater.updateAll(updateItems, null)
        updateItems.clear()
      }
      if (updatePatchItems.size() >= writingBatchSize) {
        bulkUpdateResponse = updater.mergeAll(updatePatchItems, null)
        updatePatchItems.clear()
      }

      if (bulkUpdateResponse != null) {
        if (bulkUpdateResponse.getFailedUpdates.size() > 0) {
          throw toFailedUpdateException(bulkUpdateResponse)
        }

        if (!bulkUpdateResponse.getErrors.isEmpty) {
          throw new Exception("Errors encountered in bulk update API execution. Exceptions observed:\n" + bulkUpdateResponse.getErrors.toString)
        }
      }
    })
    if (updateItems.size() > 0) {
      bulkUpdateResponse = updater.updateAll(updateItems, null)
    }
    if (updatePatchItems.size() > 0) {
      bulkUpdateResponse = updater.mergeAll(updatePatchItems, null)
    }

    if (bulkUpdateResponse != null) {
      if (bulkUpdateResponse.getFailedUpdates.size() > 0) {
        throw toFailedUpdateException(bulkUpdateResponse)
      }

      if (!bulkUpdateResponse.getErrors.isEmpty) {
        throw new Exception("Errors encountered in bulk update API execution. Exceptions observed:\n" + bulkUpdateResponse.getErrors.toString)
      }
    }
  }

  private def getDocumentsDebugString(
    connection: CosmosDBConnection,
    docs: java.util.List[String]): String =
  {
    val pkDefinitionModel = connection.getPartitionKeyDefinition

    logDebug(s"PartitionKeyDefinition: Kind: ${pkDefinitionModel.getKind.toString}")
    val version = pkDefinitionModel.getVersion
    if (version != null)
    {
      logDebug(s"PartitionKeyDefinition: Version: ${version.toString}")
    }
    logDebug(s"PartitionKeyDefinition: Paths - ${pkDefinitionModel.getPaths.mkString("|")}")

    val sb = new StringBuilder()
    docs.foreach(d =>
      {
        val doc : String = d.toString
        val internalPK = com.microsoft.azure.documentdb.bulkexecutor.internal.DocumentAnalyzer
          .extractPartitionKeyValue(doc, pkDefinitionModel)
        val effectivePK = internalPK
          .getEffectivePartitionKeyString(pkDefinitionModel, true)
        val jsonPK = internalPK
          .toJson
        sb.append(jsonPK).append("(").append(effectivePK).append(") --> ").append(doc).append(", ")
      }
    )
    sb.toString()
  }

  private def bulkImport[D: ClassTag](iter: Iterator[D],
                                      connection: CosmosDBConnection,
                                      writingBatchSize: Int,
                                      rootPropertyToSave: Option[String],
                                      upsert: Boolean,
                                      maxConcurrencyPerPartitionRange: Integer,
                                      partitionCount: Int,
                                      baseMaxMiniBatchImportSize: Int,
                                      writeThroughputBudgetPerCosmosPartition: Option[Int]): Unit = {
    // Initialize BulkExecutor
    val importer: DocumentBulkExecutor = connection.getDocumentBulkImporter

    // Set retry options to 0 to pass control to BulkExecutor
    // connection.setZeroClientRetryPolicy

    val documents = new java.util.ArrayList[String](writingBatchSize)
    val cosmosDBRowConverter = new CosmosDBRowConverter(SerializationConfig.fromConfig(connection.config))

    var budgetEvalSize: Double = 0
    var isBudgetEvalComplete: Boolean = !writeThroughputBudgetPerCosmosPartition.exists(_ > 0)
    var effectiveMaxMiniBatchImportSize: Int = baseMaxMiniBatchImportSize
    val effectivewriteThroughputBudgetPerCosmosPartition: Int = writeThroughputBudgetPerCosmosPartition.getOrElse(0)

    var bulkImportResponse: BulkImportResponse = null
    iter.foreach(item => {
      val document: Document = item match {
        case doc: Document => doc
        case row: Row =>
          if (rootPropertyToSave.isDefined) {
            new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
          } else {
            new Document(cosmosDBRowConverter.rowToJSONObject(row).toString())
          }
        case any => new Document(any.toString)
      }
      if (document.getId == null) {
        document.setId(UUID.randomUUID().toString)
      }
      documents.add(document.toJson())

      // An initial one-time bulk import is performed with the baseMaxMiniBatchImportSize and the RU consumption is collected.
      // This will take into consideration the indexed vs non-indexed target container, size of the imported docs etc:
      // The miniBatchSizeAdjustmentFactor is calculated based on the above RU consumption and the effective minibatch size is adjusted based on this.
      if (writeThroughputBudgetPerCosmosPartition.exists(_ > 0) && !isBudgetEvalComplete) {
        budgetEvalSize += document.toJson().getBytes(Charset.forName("UTF-8")).length
        if (budgetEvalSize >= baseMaxMiniBatchImportSize ) {
          bulkImportResponse = importer.importAll(documents, upsert, false, maxConcurrencyPerPartitionRange,
            baseMaxMiniBatchImportSize, partitionCount, effectivewriteThroughputBudgetPerCosmosPartition)
          if (!bulkImportResponse.getErrors.isEmpty) {
            throw new Exception("Errors encountered in bulk import API execution. Exceptions observed:\n" + bulkImportResponse.getErrors.toString)
          }
          if (!bulkImportResponse.getBadInputDocuments.isEmpty) {
            throw new Exception("Bad input documents provided to bulk import API. Bad input documents observed:\n" + bulkImportResponse.getBadInputDocuments.toString)
          }
          if (bulkImportResponse.getFailedImports.size() > 0) {
            throw toFailedImportException(bulkImportResponse, connection)
          }

          val requestUnitsConsumed = bulkImportResponse.getTotalRequestUnitsConsumed
          val miniBatchSizeAdjustmentFactor = (requestUnitsConsumed * partitionCount) / effectivewriteThroughputBudgetPerCosmosPartition
          effectiveMaxMiniBatchImportSize = (baseMaxMiniBatchImportSize / miniBatchSizeAdjustmentFactor).ceil.toInt
          isBudgetEvalComplete = true
          documents.clear()
        }
      }

      if (documents.size() >= writingBatchSize && isBudgetEvalComplete) {
        bulkImportResponse = importer.importAll(documents, upsert, false, maxConcurrencyPerPartitionRange,
          effectiveMaxMiniBatchImportSize, partitionCount, effectivewriteThroughputBudgetPerCosmosPartition)
        if (!bulkImportResponse.getErrors.isEmpty) {
          throw new Exception("Errors encountered in bulk import API execution. Exceptions observed:\n" + bulkImportResponse.getErrors.toString)
        }
        if (!bulkImportResponse.getBadInputDocuments.isEmpty) {
          throw new Exception("Bad input documents provided to bulk import API. Bad input documents observed:\n" + bulkImportResponse.getBadInputDocuments.toString)
        }
        if (bulkImportResponse.getFailedImports.size() > 0) {
          throw toFailedImportException(bulkImportResponse, connection)
        }
        documents.clear()
      }
    })
    if (documents.size() > 0) {
      bulkImportResponse = importer.importAll(documents, upsert, false, maxConcurrencyPerPartitionRange,
        effectiveMaxMiniBatchImportSize, partitionCount, effectivewriteThroughputBudgetPerCosmosPartition)
      if (!bulkImportResponse.getErrors.isEmpty) {
        throw new Exception("Errors encountered in bulk import API execution. Exceptions observed:\n" + bulkImportResponse.getErrors.toString)
      }
      if (!bulkImportResponse.getBadInputDocuments.isEmpty) {
        throw new Exception("Bad input documents provided to bulk import API. Bad input documents observed:\n" + bulkImportResponse.getBadInputDocuments.toString)
      }
      if (bulkImportResponse.getFailedImports.size() > 0) {
        throw toFailedImportException(bulkImportResponse, connection)
      }
    }
  }

  private def toFailedImportException(response: BulkImportResponse, connection: CosmosDBConnection) : Exception = {

    val failedImport = response.getFailedImports.get(0)
    val failure = failedImport.getBulkImportFailureException

    val failedImportDocs = getDocumentsDebugString(
        connection,
        failedImport.getDocumentsFailedToImport)

    val failureWithCallstack = getExceptionWithCallstack(failure)
        
    val message = "Errors encountered in bulk import API execution. " +
      "PartitionKeyDefinition: " + connection.getPartitionKeyDefinition + ", " +
      "Number of failures corresponding to exception of type: " +
      failure.getClass.getName + " = " +
      failedImport.getDocumentsFailedToImport.size() + "; " +
      "FAILURE: " + failureWithCallstack  + "; " +
      "DOCUMENT FAILED TO IMPORT: " + failedImportDocs

    logError(message)

    new Exception(message)
  }

  private def getExceptionWithCallstack(throwable: Throwable) : String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw)
    throwable.printStackTrace(pw)
    pw.flush()
    val failureWithCallstack = sw.toString()
    pw.close()

    failureWithCallstack
  }

  private def toFailedUpdateException(response: BulkUpdateResponse) : Exception = {
    val failedUpdate = response.getFailedUpdates.get(0)
    val failure = failedUpdate.getBulkUpdateFailureException

    val failedUpdateIds = failedUpdate.getFailedUpdateItems.map(_.getId)
    val failedUpdatePKValues = failedUpdate.getFailedUpdateItems.map(_.getPartitionKeyValue)
    val failedUpdateIdPKValuesList = failedUpdateIds.zip(failedUpdatePKValues).toList.mkString(", ")

    val failureWithCallstack = getExceptionWithCallstack(failure)

    val message = "Errors encountered in bulk update API execution. " +
      "Number of failures corresponding to exception of type: " +
      failure.getClass.getName + " = " + failedUpdate.getFailedUpdateItems.size + "; " +
      "FAILURE: " + failureWithCallstack + "; " +
      "The global identifier (id, pk) tuples of the failed updates are: " + failedUpdateIdPKValuesList

    logError(message)

    new Exception(message)
  }

  private def savePartition[D: ClassTag](iter: Iterator[D],
                                         config: Config,
                                         hadoopConfig: mutable.Map[String, String],
                                         partitionCount: Int,
                                         baseMaxMiniBatchImportSize: Int,
                                         writeThroughputBudgetPerCosmosPartition: Option[Int]): Iterator[D] = {
    val connection: CosmosDBConnection = CosmosDBConnection(config, hadoopConfig)
    val asyncConnection: AsyncCosmosDBConnection = new AsyncCosmosDBConnection(config)

    val isBulkImporting = config.get[String](CosmosDBConfig.BulkImport).
      getOrElse(CosmosDBConfig.DefaultBulkImport.toString).
      toBoolean

    val upsert: Boolean = config
      .getOrElse(CosmosDBConfig.Upsert, String.valueOf(CosmosDBConfig.DefaultUpsert))
      .toBoolean
    val writingBatchSize = if (isBulkImporting) {
      config.getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_BulkInsert))
        .toInt
    } else {
      config.getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_PointInsert))
        .toInt
    }

    val writingBatchDelayMs = config
      .getOrElse(CosmosDBConfig.WritingBatchDelayMs, String.valueOf(CosmosDBConfig.DefaultWritingBatchDelayMs))
      .toInt
    val rootPropertyToSave = config
      .get[String](CosmosDBConfig.RootPropertyToSave)
    val isBulkUpdating = config.get[String](CosmosDBConfig.BulkUpdate).
      getOrElse(CosmosDBConfig.DefaultBulkUpdate.toString).
      toBoolean

    val maxConcurrencyPerPartitionRange = config
      .getOrElse[String](CosmosDBConfig.BulkImportMaxConcurrencyPerPartitionRange, String.valueOf(CosmosDBConfig.DefaultBulkImportMaxConcurrencyPerPartitionRange))
      .toInt

    CosmosDBSpark.lastUpsertSetting = Some(upsert)
    CosmosDBSpark.lastWritingBatchSize = Some(writingBatchSize)

    if (iter.nonEmpty) {
      if (isBulkUpdating) {
        logDebug(s"Writing partition with bulk update")
        bulkUpdate(iter, connection, writingBatchSize)
      } else if (isBulkImporting) {
        logDebug(s"Writing partition with bulk import")
        bulkImport(
          iter,
          connection,
          writingBatchSize,
          rootPropertyToSave,
          upsert,
          maxConcurrencyPerPartitionRange,
          partitionCount,
          baseMaxMiniBatchImportSize,
          writeThroughputBudgetPerCosmosPartition)
      } else {
        logDebug(s"Writing partition with rxjava")
        asyncConnection.importWithRxJava(iter, asyncConnection, writingBatchSize, writingBatchDelayMs, rootPropertyToSave, upsert)
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
    save(dataset.rdd, writeConfig)
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
      val readConf = if (config.isDefined) {
        Config(options, config)
      } else {
        Config(session.sparkContext.getConf, options)
      }

      logInfo("Read config: " + readConf.toString)
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
      * @param config the readConfig
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
  def load(jsc: JavaSparkContext): JavaCosmosDBRDD = builder().javaSparkContext(jsc).build().toJavaRDD

  /**
    * Load data from CosmosDB
    *
    * @param jsc the Spark context containing the CosmosDB connection configuration
    * @return a CosmosDBRDD
    */
  def load(jsc: JavaSparkContext, readConfig: Config): JavaCosmosDBRDD =
    builder().javaSparkContext(jsc).config(readConfig).build().toJavaRDD

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
  * '''Note:''' Creation of the class should be via [[CosmosDBSpark$.Builder]].
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
  def toJavaRDD: JavaCosmosDBRDD = rdd.toJavaRDD

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
    val rowRDD = new CosmosDBRowConverter().asRow(schema, rdd)
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

