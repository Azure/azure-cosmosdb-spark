package com.microsoft.azure.cosmosdb.spark

case class ItemSchema (columns :  Array[Column], schemaType : String)

case class Column (name:  String, dataType : String, defaultValue : Object )
