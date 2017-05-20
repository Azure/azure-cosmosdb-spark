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

import java.util.concurrent._
import java.util.function.Consumer

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.{CosmosDBRDD, _}
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.documentdb._
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * The CosmosDBSpark allow fast creation of RDDs, DataFrames or Datasets from CosmosDBSpark.
  *
  * @since 1.0
  */
object CosmosDBSpark {

  /**
   * The default source string for creating DataFrames from CosmosDB
   */
  val defaultSource = classOf[DefaultSource].getCanonicalName

  /**
    * For verfication purpose
    */
  var lastUpsertSetting: Option[Boolean] = _

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
    var connection = CosmosDBConnection(writeConfig)
    val upsert: Boolean = writeConfig
      .getOrElse(CosmosDBConfig.Upsert, String.valueOf(CosmosDBConfig.DefaultUpsert))
      .toBoolean

    CosmosDBSpark.lastUpsertSetting = Some(upsert)

    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      val executorService = Executors.newCachedThreadPool()
      var callables = new ListBuffer[Callable[Document]]()
      iter.foreach(item => {
        callables.append(new Callable[Document] {
          override def call(): Document = {
            if (upsert)
              connection.upsertDocument(item.asInstanceOf[Document], null)
            else
              connection.createDocument(item.asInstanceOf[Document], null)
          }
        })
      })
      val futures = executorService.invokeAll(callables.asJava)
      futures.forEach(new Consumer[Future[Document]] {
        override def accept(t: Future[Document]): Unit = t.get()
      })
    })
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
  def save[D](dataset: Dataset[D]): Unit = save(dataset, Config(dataset.sparkSession.sparkContext.getConf))

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
  def save[D](dataset: Dataset[D], writeConfig: Config): Unit = {
    var documentRDD: RDD[Document] = dataset.toDF().rdd.map(row => CosmosDBRowConverter.rowToDocument(row))
    CosmosDBSpark.save(documentRDD, writeConfig)
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

