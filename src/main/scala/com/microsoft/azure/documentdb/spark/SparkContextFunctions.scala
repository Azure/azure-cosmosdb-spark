package com.microsoft.azure.documentdb.spark

import java.io.Serializable

import com.microsoft.azure.documentdb.spark.config._
import com.microsoft.azure.documentdb.spark.rdd._
import org.apache.spark.SparkContext

case class SparkContextFunctions(@transient sc: SparkContext) extends Serializable{
  def loadFromDocumentDB(config: Config = Config(sc)): DocumentDBRDD = {
    DocumentDBSpark.builder().sparkContext(sc).config(config).build().toRDD
  }
}
