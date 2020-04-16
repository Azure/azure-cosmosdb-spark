package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.cosmosdb.spark.config.Config

trait SparkTokenResolver {
  def initialize(config: Config): Unit
}
