package com.microsoft.azure.cosmosdb.spark.util

import com.microsoft.azure.cosmosdb.{TokenResolver}

object CosmosUtils extends Serializable {

  def getTokenResolverFromClassName(className: String, constructorArgs: AnyRef*): TokenResolver = {
    val argsClassSeq = constructorArgs.map(e => e.getClass)
    Class.forName(className).getDeclaredConstructor(argsClassSeq:_*).newInstance(constructorArgs:_*).asInstanceOf[TokenResolver]
  }

}
