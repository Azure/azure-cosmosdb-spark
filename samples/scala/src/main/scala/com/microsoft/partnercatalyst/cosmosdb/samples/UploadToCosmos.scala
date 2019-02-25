package com.microsoft.partnercatalyst.cosmosdb.samples

import java.io.{File, PrintWriter}

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UploadToCosmos {

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val conf = new SparkConf()
      .setAppName(appName)
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val spark = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()

    // Originally referenced in https://github.com/dennyglee/databricks/blob/master/notebooks/Users/denny%40databricks.com/flights/On-Time%20Flight%20Performance.py
    val writer = new PrintWriter(new File("airports.dat"))
    scala.io.Source.fromURL("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat").getLines().foreach(writer.println)

    import spark.implicits._
    val airports = spark.read.csv("airports.dat").map(row => {
      Airport(
        row.getString(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        row.getString(5),
        SafeStringToDouble(row.getString(6)),
        SafeStringToDouble(row.getString(7)),
        SafeStringToDouble(row.getString(8)),
        SafeStringToDouble(row.getString(9)),
        row.getString(10),
        row.getString(11),
        row.getString(12),
        row.getString(13)
      )
    })
    airports.show()

    val cosmosConfig = Config(Map(
      CosmosDBConfig.Endpoint -> sys.env("COSMOS_DB_ENDPOINT"),
      CosmosDBConfig.Masterkey -> sys.env("COSMOS_DB_MASTER_KEY"),
      CosmosDBConfig.Database -> "samples",
      CosmosDBConfig.Collection -> "airports",
      CosmosDBConfig.SamplingRatio -> "1.0",
      CosmosDBConfig.QueryMaxRetryOnThrottled -> "10",
      CosmosDBConfig.QueryMaxRetryWaitTimeSecs -> "10"
    ))

    airports.write.mode(SaveMode.Append).cosmosDB(cosmosConfig)
  }

  object SafeStringToDouble extends Serializable {
    def apply(str: String): Double = {
      try {
        if (str == null) 0.0 else str.toDouble
      } catch {
        case nfe: NumberFormatException => 0.0
      }
    }
  }

}


