package com.microsoft.azure.cosmosdb.spark

import java.io.Serializable

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.DocumentDBRDD
import org.apache.spark.SparkContext

case class SparkContextFunctions(@transient sc: SparkContext) extends Serializable{
  def loadFromDocumentDB(config: Config = Config(sc)): DocumentDBRDD = {
    DocumentDBSpark.builder().sparkContext(sc).config(config).build().toRDD
  }
}
