package com.microsoft.azure.cosmosdb.spark

case class ItemSchema (columns :  Array[ItemColumn], schemaType : String)

case class ItemColumn(name:  String, dataType : String, defaultValue : Object )
