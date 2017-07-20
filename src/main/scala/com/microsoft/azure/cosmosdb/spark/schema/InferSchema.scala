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

import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig
import com.microsoft.azure.cosmosdb.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.types.{DataType, _}

import scala.reflect.runtime.universe._

/**
  * A type marking fields in the document structure to schema translation process that should be skipped.
  *
  * The main example use case is skipping a field with an empty array as the value.
  */
private class SkipFieldType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "SkipFieldType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1

  override def asNullable: SkipFieldType = this
}

private case object SkipFieldType extends SkipFieldType

object InferSchema {

  /**
    * Gets a schema for the specified CosmosDB collection. It is required that the
    * collection provides Documents.
    *
    * Utilizes the `\$sample` aggregation operator in server versions 3.2+. Older versions take a sample of the most recent 10k documents.
    *
    * @param sc the spark context
    * @return the schema for the collection
    */
  def apply(sc: SparkContext): StructType = apply(CosmosDBSpark.load(sc))

  /**
    * Gets a schema for the specified CosmosDB collection. It is required that the
    * collection provides Documents.
    *
    * Utilizes the `\$sample` aggregation operator in server versions 3.2+. Older versions take a sample of the most recent 10k documents.
    *
    * @param rdd the CosmosDB to be sampled
    * @return the schema for the collection
    */
  def apply(rdd: CosmosDBRDD): StructType = {
    val sampleData = rdd.sparkContext.parallelize(rdd.takeSample(withReplacement = false, CosmosDBConfig.DefaultSampleSize))
    val samplingRatio = rdd.sparkContext.getConf.getDouble(CosmosDBConfig.SamplingRatio, CosmosDBConfig.DefaultSamplingRatio)

    CosmosDBSchema(sampleData, samplingRatio).schema()
  }

  /**
    * Remove StructTypes with no fields or SkipFields
    */
  private def canonicalizeType: DataType => Option[DataType] = {
    case at@ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        if field.dataType != SkipFieldType
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }
    case other => Some(other)
  }

  /**
    * Gets the matching DataType for the input DataTypes.
    *
    * For simple types, returns a ConflictType if the DataTypes do not match.
    *
    * For complex types:
    * - ArrayTypes: if the DataTypes of the elements cannot be matched, then
    * an ArrayType(ConflictType, true) is returned.
    * - StructTypes: for any field on which the DataTypes conflict, the field
    * value is replaced with a ConflictType.
    *
    * @param t1 the DataType of the first element
    * @param t2 the DataType of the second element
    * @return the DataType that matches on the input DataTypes
    */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonType(t1, t2).getOrElse {
    //TypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))
        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
        // SkipFieldType Types
        case (s: SkipFieldType, dataType: DataType) => dataType
        case (dataType: DataType, s: SkipFieldType) => dataType
      }
    }
  }

  def reflectSchema[T <: Product : TypeTag](): Option[StructType] = {
    typeOf[T] match {
      case x if x == typeOf[Nothing] => None
      case _ => Some(ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType])
    }
  }

  def reflectSchema[T](beanClass: Class[T]): StructType = JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]
}
