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

import java.util

import com.microsoft.azure.documentdb.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  * Knows the way to provide some Data Source schema
  */
trait SchemaProvider {

  /**
    * Provides the schema for current implementation of Data Source
    *
    * @return schema
    */
  def schema(): StructType

}

case class CosmosDBSchema[T <: RDD[Document]](
                                                 rdd: T,
                                                 samplingRatio: Double) extends SchemaProvider with Serializable {

  override def schema(): StructType = {
    val schemaData =
      if (samplingRatio > 0.99) rdd
      else rdd.sample(withReplacement = false, samplingRatio, 1)

    val flatMap = schemaData.flatMap {
      dbo =>
        val doc: Map[String, AnyRef] = dbo.getHashMap().asScala.toMap
        val fields = doc.mapValues(f => convertToStruct(f))
        fields
    }

    val reduced = flatMap.reduceByKey(compatibleType)

    val structFields = reduced.aggregate(Seq[StructField]())({
      case (fields, (name, tpe)) =>
        val newType = tpe match {
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case other => other
        }
        fields :+ StructField(name, newType)
    }, (oldFields, newFields) => oldFields ++ newFields)

    StructType(structFields)
  }

  private def convertToStruct(dataType: Any): DataType = dataType match {
    case array: util.ArrayList[_] =>
      val arrayType: immutable.Seq[DataType] = array.asScala.toList.map(x => convertToStruct(x)).distinct
      ArrayType(if (arrayType.nonEmpty) arrayType.head else NullType, arrayType.contains(NullType))
    case hm: util.HashMap[_, _] =>
      val fields = hm.asInstanceOf[util.HashMap[String, AnyRef]].asScala.toMap.map {
        case (k, v) => StructField(k, convertToStruct(v))
      }.toSeq
      StructType(fields)
    case bo: Document =>
      val fields = bo.getHashMap().asScala.toMap.map {
        case (k, v) =>
          StructField(k, convertToStruct(v))
      }.toSeq
      StructType(fields)
    case elem =>
      elemType(elem)
  }

  /**
    * It looks for the most compatible type between two given DataTypes.
    * i.e.: {{{
    *   val dataType1 = IntegerType
    *   val dataType2 = DoubleType
    *   assert(compatibleType(dataType1,dataType2)==DoubleType)
    * }}}
    *
    * @param t1 First DataType to compare
    * @param t2 Second DataType to compare
    * @return Compatible type for both t1 and t2
    */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    //TypeCoercion.findTightestCommonTypeOfTwo(t1, t2) match {
    TypeCoercion.findTightestCommonType(t1, t2) match {
      case Some(commonType) => commonType

      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) =>
            val newFields = (fields1 ++ fields2)
              .groupBy(field => field.name)
              .map { case (name, fieldTypes) =>
                val dataType = fieldTypes
                  .map(field => field.dataType)
                  .reduce(compatibleType)
                StructField(name, dataType, nullable = true)

              }
            StructType(newFields.toSeq.sortBy(_.name))

          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(
              compatibleType(elementType1, elementType2),
              containsNull1 || containsNull2)

          case (_, _) => StringType
        }
    }
  }

  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.contains(null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements
        .map(convertToStruct)
        .reduce(compatibleType)
      ArrayType(elementType, containsNull)
    }
  }

  private def elemType: PartialFunction[Any, DataType] = {
    case obj: Boolean => BooleanType
    case obj: Array[Byte] => BinaryType
    case obj: String => StringType
    case obj: UTF8String => StringType
    case obj: Byte => ByteType
    case obj: Short => ShortType
    case obj: Int => IntegerType
    case obj: Long => LongType
    case obj: Float => FloatType
    case obj: Double => DoubleType
    case obj: java.sql.Date => DateType
    case obj: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
    case obj: Decimal => DecimalType.SYSTEM_DEFAULT
    case obj: java.sql.Timestamp => TimestampType
    case null => NullType
    case date: java.util.Date => TimestampType
    case _ => StringType
  }
}
