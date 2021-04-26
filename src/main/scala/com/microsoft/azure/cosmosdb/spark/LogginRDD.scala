package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import com.microsoft.azure.documentdb.{Document, PartitionKeyDefinition}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

private[spark] class LoggingRDD[D: ClassTag]
(
    innerRDD: RDD[D],
    logger: Option[IteratorLogger],
    partitionKey: PartitionKeyDefinition,
    rootPropertyToSave: Option[String],
    cosmosDBRowConverter: CosmosDBRowConverter
) extends RDD[Document](innerRDD) {
  override def compute(split: Partition, context: TaskContext): Iterator[Document] = {
    LoggingIterator.createLoggingAndConvertingIterator(
      firstParent[D].iterator(split, context),
      logger,
      partitionKey,
      rootPropertyToSave,
      cosmosDBRowConverter
    )
  }

  override protected def getPartitions: Array[Partition] = firstParent[D].partitions
}
