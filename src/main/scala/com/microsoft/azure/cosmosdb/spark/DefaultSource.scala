/**
  * The MIT License (MIT)
  * Copyright (c) 2016 Microsoft Corporation
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package com.microsoft.azure.cosmosdb.spark

import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRelation
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with CosmosDBLoggingTrait {

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Predef.Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, None)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Predef.Map[String, String],
                               schema: StructType): BaseRelation = {
    createRelation(sqlContext, parameters, Some(schema))
  }

  private def createRelation(
                            sqlContext: SQLContext,
                            parameters: Predef.Map[String, String],
                            schema: Option[StructType]
                            ): BaseRelation = {
    new CosmosDBRelation(Config(sqlContext.sparkContext.getConf, parameters), schema)(sqlContext.sparkSession)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Predef.Map[String, String],
                               data: DataFrame): BaseRelation = {

    val config: Config = Config(sqlContext.sparkContext.getConf, parameters)
    val connection: CosmosDBConnection = CosmosDBConnection(config, HdfsUtils.getConfigurationMap(sqlContext.sparkSession.sparkContext.hadoopConfiguration))
    val isEmptyCollection: Boolean = connection.isDocumentCollectionEmpty
    mode match{
      case Append =>
        CosmosDBSpark.save(data, config)
      case Overwrite =>
        var upsertConfig: collection.Map[String, String] = config.asOptions
        upsertConfig -= CosmosDBConfig.Upsert
        upsertConfig += CosmosDBConfig.Upsert -> String.valueOf(true)
        CosmosDBSpark.save(data, Config(upsertConfig))
      case ErrorIfExists  =>
        if (isEmptyCollection)
          CosmosDBSpark.save(data, config)
        else
          throw new UnsupportedOperationException("Writing in a non-empty collection.")
      case Ignore =>
        if (isEmptyCollection)
          CosmosDBSpark.save(data, config)
        else
          logInfo("Ignore writing to non empty collection.")
      case default =>
        throw new UnsupportedOperationException(s"Unsupported SaveMode $default")
    }

    createRelation(sqlContext, parameters, Some(data.schema))
  }
}
