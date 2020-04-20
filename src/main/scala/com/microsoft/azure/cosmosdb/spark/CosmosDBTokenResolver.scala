package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.TokenResolver

trait CosmosDBTokenResolver extends TokenResolver {
  def initialize(config: Config): Unit
}
