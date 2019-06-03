package myproject.scala

// Databricks notebook source
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local").appName("TestApplication").getOrCreate()

    val edgesConfig = Config(Map(
      "Endpoint" -> "<Endpoint>",
      "Masterkey" -> "<key>",
      "Database" -> "TestDB",
      "Collection" -> "graph-logging",
      "query_pagesize" -> "1000",
      "consistencylevel" -> "Eventual",
      "query_custom" -> "select * from c where c._isEdge = true"
    ))

    var edges = spark.read.cosmosDB(edgesConfig)
    edges.createOrReplaceTempView("edges")
    edges = edges.withColumnRenamed("_vertexId", "src")
    edges = edges.withColumnRenamed("_sink", "dst")
    print(edges.schema)


    val verticesConfig = Config(Map(
      "Endpoint" -> "<Endpoint>",
      "Masterkey" -> "<key>",
      "Database" -> "TestDB",
      "Collection" -> "graph-logging",
      "query_pagesize" -> "1000",
      "consistencylevel" -> "Eventual",
      "query_custom" -> "select * from c where NOT IS_DEFINED(c._isEdge)"
    ))

    val vertices = spark.read.cosmosDB(verticesConfig)
    vertices.createOrReplaceTempView("vertices")
    print(vertices.schema)

    import org.graphframes.GraphFrame

    val g = GraphFrame(vertices, edges)
    g.inDegrees.show()

  }
}