package com.microsoft.azure.cosmosdb.spark

import java.io.Serializable

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.CosmosDBRDD
import org.apache.spark.SparkContext

case class SparkContextFunctions(@transient sc: SparkContext) extends Serializable{
  def loadFromCosmosDB(config: Config = Config(sc)): CosmosDBRDD = {
    CosmosDBSpark.builder().sparkContext(sc).config(config).build().toRDD
  }
}
