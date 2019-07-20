package com.microsoft.azure.cosmosdb.spark


/**
  * Class encapsulating the schema for a document type.
  */
case class ItemSchema (columns :  Array[ItemColumn], schemaType : String)

case class ItemColumn(name:  String, dataType : String, defaultValue : Object )
