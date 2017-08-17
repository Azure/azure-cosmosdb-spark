package com.microsoft.partnercatalyst.cosmosdb.samples

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CountByCountry {

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val spark = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()

    val cosmosConfig = Config(Map(
      CosmosDBConfig.Endpoint -> sys.env("COSMOS_DB_ENDPOINT"),
      CosmosDBConfig.Masterkey -> sys.env("COSMOS_DB_MASTER_KEY"),
      CosmosDBConfig.Database -> "samples",
      CosmosDBConfig.Collection -> "airports",
      CosmosDBConfig.ConnectionMode -> "Gateway"
    ))

    val airportsDF = spark.read.cosmosDB[Airport](cosmosConfig)
    airportsDF.cache()
    airportsDF.show()

    airportsDF.createOrReplaceTempView("airports")
    val counts = airportsDF.sqlContext.sql("select airports.country, count(1) from airports group by airports.country")
    counts.show()
  }

}
