package com.microsoft.partnercatalyst.cosmosdb.samples

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.rdd.RDD._
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
    import spark.implicits._

    val cosmosConfig = Config(Map(
      CosmosDBConfig.Endpoint -> sys.env("COSMOS_DB_ENDPOINT"),
      CosmosDBConfig.Masterkey -> sys.env("COSMOS_DB_MASTER_KEY"),
      CosmosDBConfig.Database -> "samples",
      CosmosDBConfig.Collection -> "airports",
      CosmosDBConfig.ConnectionMode -> "Gateway"
    ))

    val airportsDF = spark.read.cosmosDB(cosmosConfig).as[Airport]
    airportsDF.cache()
    airportsDF.show()

    // Count number of aiports by country using reduceByKey from the incoming RDD
    val rddStart = System.currentTimeMillis()
    val reducedByKey = airportsDF.map(r=>(r.country, 1))
      .rdd
      .reduceByKey(_+_)
      .toDF("country", "airport_count")
    reducedByKey.show()
    val rddStop = System.currentTimeMillis()

    airportsDF.createOrReplaceTempView("airports")
    val airportsTempView = airportsDF.sqlContext.table("airports")
    airportsTempView.show()

    // Perform the same count using SQL
    val sqlStart = System.currentTimeMillis()
    val counts = airportsDF.sqlContext.sql("select airports.country, count(1) as airport_count from airports group by airports.country")
    counts.show()
    val sqlStop = System.currentTimeMillis()

    // Once again, perform the count by fetching from the temp table and calling reduceByKey.
    val rdd2Start = System.currentTimeMillis()
    airportsTempView.select("country", "airportId")
      .rdd
      .map(row=>(row.getString(1), 1))
      .reduceByKey(_+_)
      .toDF("country", "airport_count")
      .show()
    val rdd2Stop = System.currentTimeMillis()

    println(s"sql time in millis: ${sqlStop - sqlStart}")
    println(s"rdd time in millis: ${rddStop - rddStart}")
    println(s"rdd2 time in millis: ${rdd2Stop - rdd2Start}")
  }

}
