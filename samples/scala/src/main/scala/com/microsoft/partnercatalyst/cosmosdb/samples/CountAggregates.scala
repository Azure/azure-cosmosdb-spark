package com.microsoft.partnercatalyst.cosmosdb.samples

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CountAggregates {

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val spark = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()
    import spark.implicits._

    sc.setLogLevel("ERROR")

    val configMap = Map[String, String](
      CosmosDBConfig.Endpoint -> sys.env("COSMOS_DB_ENDPOINT"),
      CosmosDBConfig.Masterkey -> sys.env("COSMOS_DB_MASTER_KEY"),
      CosmosDBConfig.Database -> "samples",
      CosmosDBConfig.Collection -> "airports",
      CosmosDBConfig.ConnectionMode -> "Gateway"
    )
    val cosmosConfig = Config(configMap)

    val airportsDF = spark.read.cosmosDB(cosmosConfig).as[Airport]
    airportsDF.createOrReplaceTempView("airports")

    spark.sqlContext.sql("select airports.country, count(1) as airport_count from airports group by airports.country")
      .createOrReplaceTempView("airport_counts")

    spark.sqlContext.sql("select min(airport_count), max(airport_count), avg(airport_count), stddev_pop(airport_count) from airport_counts")
      .show()
  }
}
