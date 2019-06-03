package com.microsoft.azure.cosmosdb.spark.schema

import com.microsoft.azure.cosmosdb.spark.ItemSchema
import com.microsoft.azure.documentdb.Document

class ItemSchemaBuilder {

  private def executePreSave(schemaDocument : ItemSchema, item : Document): Unit =
  {
    item.set("documentSchema", schemaDocument.schemaType)
  }

}
