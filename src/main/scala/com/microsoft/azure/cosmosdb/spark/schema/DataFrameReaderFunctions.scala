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
package com.microsoft.azure.cosmosdb.spark.schema

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.{DefaultSource, LoggingTrait}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import scala.reflect.runtime.universe._

object DataFrameReaderFunctions {
  private val cachedData: ConcurrentMap[String, DataFrame] = new ConcurrentHashMap[String, DataFrame]()
}

private[spark] case class DataFrameReaderFunctions(@transient dfr: DataFrameReader) extends LoggingTrait {

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
    * determine the type.
    *
    * @tparam T The optional type of the data from CosmosDB
    * @return DataFrame
    */
  def cosmosDB[T <: Product : TypeTag](): DataFrame = createCosmosDBDataFrame(InferSchema.reflectSchema[T](), None)

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
    * determine the type.
    *
    * @param readConfig any connection read configuration overrides. Overrides the configuration set in [[org.apache.spark.SparkConf]]
    * @tparam T The optional type of the data from CosmosDB
    * @return DataFrame
    */
  def cosmosDB[T <: Product : TypeTag](readConfig: Config): DataFrame =
    createCosmosDBDataFrame(InferSchema.reflectSchema[T](), Some(readConfig))

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema the schema definition
    * @return DataFrame
    */
  def cosmosDB(schema: StructType): DataFrame = createCosmosDBDataFrame(Some(schema), None)

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema     the schema definition
    * @param readConfig any custom read configuration
    * @return DataFrame
    */
  def cosmosDB(schema: StructType, readConfig: Config): DataFrame = createCosmosDBDataFrame(Some(schema), Some(readConfig))

  private def createDataFrame(schema: Option[StructType], readConfig: Option[Config]): DataFrame = {
    var cachingMode: Int = 0
    var database: String = StringUtils.EMPTY
    var collection: String = StringUtils.EMPTY
    var collectionCacheKey: String = StringUtils.EMPTY

    if (readConfig.isDefined) {
      cachingMode = readConfig.get
        .get[String](CosmosDBConfig.CachingModeParam)
        .getOrElse(CosmosDBConfig.DefaultCacheMode.toString)
        .toInt

      database = readConfig.get.getOrElse[String](CosmosDBConfig.Database, StringUtils.EMPTY)
      collection = readConfig.get.getOrElse[String](CosmosDBConfig.Collection, StringUtils.EMPTY)
      collectionCacheKey = s"dbs/$database/colls/$collection"
    }

    if (cachingMode == 1) { // Cache
      if (DataFrameReaderFunctions.cachedData.containsKey(collectionCacheKey)) {
        return DataFrameReaderFunctions.cachedData.get(collectionCacheKey)
      }
    }

    val builder = dfr.format(classOf[DefaultSource].getPackage.getName)
    if (schema.isDefined) dfr.schema(schema.get)
    if (readConfig.isDefined) dfr.options(readConfig.get.asOptions)
    val df = builder.load()

    if (cachingMode == 1 || cachingMode == 2) { // Cache or Refresh Cache
      df.cache()
      DataFrameReaderFunctions.cachedData.put(collection, df)
    }

    df
  }

  private def createCosmosDBDataFrame(schema: Option[StructType], readConfig: Option[Config]): DataFrame = {
    if (readConfig.isDefined) {
      val incrementalView: Boolean = readConfig.get
        .get[String](CosmosDBConfig.IncrementalView)
        .getOrElse(CosmosDBConfig.DefaultIncrementalView.toString)
        .toBoolean

      if (incrementalView) {
        val dfConfig = Config(readConfig.get.asOptions
          .-(CosmosDBConfig.ReadChangeFeed)
          .-(CosmosDBConfig.RollingChangeFeed)
          .-(CosmosDBConfig.CachingModeParam)
          .+((CosmosDBConfig.CachingModeParam, "1")))
        val df = createDataFrame(schema, Some(dfConfig))

        val changeFeedConfig = Config(dfConfig.asOptions
          .+((CosmosDBConfig.ReadChangeFeed, "true"))
          .-(CosmosDBConfig.CachingModeParam))
        val changeFeedDf = createDataFrame(schema, Some(changeFeedConfig))

        df.union(changeFeedDf)
      } else {
        createDataFrame(schema, readConfig)
      }
    } else {
      createDataFrame(schema, readConfig)
    }
  }
}