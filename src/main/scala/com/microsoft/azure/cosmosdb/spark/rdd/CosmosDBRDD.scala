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
package com.microsoft.azure.cosmosdb.spark.rdd

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.partitioner.{CosmosDBPartition, CosmosDBPartitioner}
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.documentdb._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class CosmosDBRDD(
                     spark: SparkSession,
                     config: Config,
                     maxItems: Option[Long] = None,
                     partitioner: CosmosDBPartitioner = new CosmosDBPartitioner(),
                     requiredColumns: Array[String] = Array(),
                     filters: Array[Filter] = Array())
  extends RDD[Document](spark.sparkContext, deps = Nil) {

  // Keep a copy of hadoop config for hdfs file handling
  // It's a Map because Configuration is not serializable
  private val hadoopConfig: mutable.Map[String, String] = HdfsUtils.getConfigurationMap(sparkContext.hadoopConfiguration)


  private def cosmosDBSpark = {
    CosmosDBSpark(spark, config)
  }

  override def toJavaRDD(): JavaCosmosDBRDD = JavaCosmosDBRDD(this)

  override def getPartitions: Array[Partition] = {
    partitioner.computePartitions(config)
  }

  /**
    * Creates a `DataFrame` based on the schema derived from the optional type.
    *
    * '''Note:''' Prefer [[toDS[T<:Product]()*]] as computations will be more efficient.
    * The rdd must contain an `_id` for CosmosDB versions < 3.2.
    *
    * @tparam T The optional type of the data from CosmosDB, if not provided the schema will be inferred from the collection
    * @return a DataFrame
    */
  def toDF[T <: Product : TypeTag](): DataFrame = cosmosDBSpark.toDF[T]()

  /**
    * Creates a `DataFrame` based on the schema derived from the bean class.
    *
    * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as computations will be more efficient.
    *
    * @param beanClass encapsulating the data from CosmosDB
    * @tparam T The bean class type to shape the data from CosmosDB into
    * @return a DataFrame
    */
  def toDF[T](beanClass: Class[T]): DataFrame = cosmosDBSpark.toDF(beanClass)

  /**
    * Creates a `DataFrame` based on the provided schema.
    *
    * @param schema the schema representing the DataFrame.
    * @return a DataFrame.
    */
  def toDF(schema: StructType): DataFrame = cosmosDBSpark.toDF(schema)

  /**
    * Creates a `Dataset` from the collection strongly typed to the provided case class.
    *
    * @tparam T The type of the data from CosmosDB
    * @return
    */
  def toDS[T <: Product : TypeTag](): Dataset[T] = cosmosDBSpark.toDS[T]()

  /**
    * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
    *
    * @tparam T The type of the data from CosmosDB
    * @return
    */
  def toDS[T](beanClass: Class[T]): Dataset[T] = cosmosDBSpark.toDS[T](beanClass)

  override def compute(
                        partition: Partition,
                        context: TaskContext): Iterator[Document] = {

    partition match {
      case cosmosDBPartition: CosmosDBPartition =>
        logInfo(s"CosmosDBRDD:compute: Start CosmosDBRDD compute task for partition key range id ${cosmosDBPartition.partitionKeyRangeId}")

        val completionListener: TaskCompletionListener = new TaskCompletionListener() {
          override def onTaskCompletion(context: TaskContext): Unit =
            logInfo(s"CosmosDBRDD:compute: CosmosDBRDD compute task completed for partition key range id ${cosmosDBPartition.partitionKeyRangeId}")
        }
        context.addTaskCompletionListener(completionListener)

        new CosmosDBRDDIterator(
          hadoopConfig,
          context,
          cosmosDBPartition,
          config,
          maxItems,
          requiredColumns,
          filters)
    }
  }
}
