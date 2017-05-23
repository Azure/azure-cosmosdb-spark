package com.microsoft.azure.cosmosdb.spark.rdd

import com.microsoft.azure.documentdb.Document
import com.microsoft.azure.cosmosdb.spark.DefaultHelper.DefaultsTo
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * :: DeveloperApi ::
  *
  * Functions for RDD's that allow the data to be saved to CosmosDB.
  *
  * @param rdd the rdd
  * @param e the implicit datatype of the rdd
  * @param ct the implicit ClassTag of the datatype of the rdd
  * @tparam D the type of data in the RDD
  * @since 1.0
  */
@DeveloperApi
case class CosmosRDDFunctions[D](rdd: RDD[D])(implicit e: D DefaultsTo Document, ct: ClassTag[D]) {

  /**
    * Saves the RDD data to CosmosDB using the given `WriteConfig`
    *
    * @param config the optional [[com.microsoft.azure.cosmosdb.spark.config]] to use
    * @return the rdd
    */
  def saveToCosmosDB(config: Config = Config(rdd.sparkContext)): Unit = CosmosDBSpark.save(rdd, config)

}
