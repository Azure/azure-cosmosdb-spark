package com.microsoft.azure.cosmosdb.spark.util

import com.microsoft.azure.cosmosdb.spark.CosmosDBTokenResolver

object CosmosUtils extends Serializable {

  def getTokenResolverFromClassName(className: String, constructorArgs: AnyRef*): CosmosDBTokenResolver = {
    val argsClassSeq = constructorArgs.map(e => e.getClass)
    Class.forName(className).getDeclaredConstructor(argsClassSeq:_*).newInstance(constructorArgs:_*).asInstanceOf[CosmosDBTokenResolver]
  }

}
