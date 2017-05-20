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

import com.microsoft.azure.cosmosdb.spark.{DefaultSource, LoggingTrait}
import com.microsoft.azure.cosmosdb.spark.config._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import scala.reflect.runtime.universe._

private[spark] case class DataFrameReaderFunctions(@transient dfr: DataFrameReader) extends LoggingTrait {

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
    * determine the type.
    *
    * @tparam T The optional type of the data from DocumentDB
    * @return DataFrame
    */
  def DocumentDB[T <: Product : TypeTag](): DataFrame = createDataFrame(InferSchema.reflectSchema[T](), None)

  /**
    * Creates a [[DataFrame]] through schema inference via the `T` type, otherwise will sample the collection to
    * determine the type.
    *
    * @param readConfig any connection read configuration overrides. Overrides the configuration set in [[org.apache.spark.SparkConf]]
    * @tparam T The optional type of the data from DocumentDB
    * @return DataFrame
    */
  def DocumentDB[T <: Product : TypeTag](readConfig: Config): DataFrame =
    createDataFrame(InferSchema.reflectSchema[T](), Some(readConfig))

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema the schema definition
    * @return DataFrame
    */
  def DocumentDB(schema: StructType): DataFrame = createDataFrame(Some(schema), None)

  /**
    * Creates a [[DataFrame]] with the set schema
    *
    * @param schema     the schema definition
    * @param readConfig any custom read configuration
    * @return DataFrame
    */
  def DocumentDB(schema: StructType, readConfig: Config): DataFrame = createDataFrame(Some(schema), Some(readConfig))

  private def createDataFrame(schema: Option[StructType], readConfig: Option[Config]): DataFrame = {
    val builder = dfr.format(classOf[DefaultSource].getPackage.getName)
    if (schema.isDefined) dfr.schema(schema.get)
    if (readConfig.isDefined) dfr.options(readConfig.get.asOptions)
    builder.load()
  }
}