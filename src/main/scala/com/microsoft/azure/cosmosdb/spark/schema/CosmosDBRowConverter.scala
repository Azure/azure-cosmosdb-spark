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

import java.sql.{Date, Timestamp}
import java.util

import com.microsoft.azure.documentdb._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, _}
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/**
  * Knows how to map from some Data Source native RDD to an {{{RDD[Row]}}}
  *
  * @tparam T Original RDD type
  */
trait RowConverter[T] {

  /**
    * Given a known schema,
    * it maps an RDD of some specified type to an {{{RDD[Row}}}
    *
    * @param schema RDD native schema
    * @param rdd    Current native RDD
    * @return A brand new RDD of Spark SQL Row type.
    */
  def asRow(schema: StructType, rdd: RDD[T]): RDD[Row]

}


object CosmosDBRowConverter extends RowConverter[Document]
  with JsonSupport
  with Serializable {

  def asRow(schema: StructType, rdd: RDD[Document]): RDD[Row] = {
    rdd.map { record =>
      recordAsRow(documentToMap(record), schema)
    }
  }

  def asRow(schema: StructType, array: Array[Document]): Array[Row] = {
    array.map { record =>
      recordAsRow(documentToMap(record), schema)
    }
  }

  def recordAsRow(
                   json: Map[String, AnyRef],
                   schema: StructType): Row = {

    val values: Seq[Any] = schema.fields.map {
      case StructField(name, et, _, mdata)
        if (mdata.contains("idx") && mdata.contains("colname")) =>
          val colName = mdata.getString("colname")
          val idx = mdata.getLong("idx").toInt
          json.get(colName).flatMap(v => Option(v)).map(toSQL(_, ArrayType(et, true))).collect {
            case elemsList: Seq[_] if ((0 until elemsList.size) contains idx) => elemsList(idx)
          } orNull
      case StructField(name, dataType, _, _) =>
        json.get(name).flatMap(v => Option(v)).map(toSQL(_, dataType)).orNull
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  def toSQL(value: Any, dataType: DataType): Any = {
    Option(value).map { value =>
      (value, dataType) match {
        case (list: List[AnyRef@unchecked], ArrayType(elementType, _)) =>
          null
        case (_, struct: StructType) =>
          val jsonMap: Map[String, AnyRef] = value match {
            case doc: Document => documentToMap(doc)
            case hm: util.HashMap[_, _] => hm.asInstanceOf[util.HashMap[String, AnyRef]].asScala.toMap
          }
          recordAsRow(jsonMap, struct)
        case (_, map: MapType) =>
          (value match {
            case document: Document => documentToMap(document)
            case _ => value.asInstanceOf[java.util.HashMap[String, AnyRef]].asScala.toMap
          }).map(element => (toSQL(element._1, map.keyType), toSQL(element._2, map.valueType)))
        case (_, array: ArrayType) =>
          value.asInstanceOf[java.util.ArrayList[AnyRef]].asScala.map(element => toSQL(element, array.elementType)).toArray
        case (_, binaryType: BinaryType) =>
          value.asInstanceOf[java.util.ArrayList[Int]].asScala.map(x => x.toByte).toArray
        case _ =>
          //Assure value is mapped to schema constrained type.
          enforceCorrectType(value, dataType)
      }
    }.orNull
  }

  def rowToDocument(row: Row): Document = {
    var document: Document = new Document()
    row.schema.fields.zipWithIndex.foreach({
      case (field, i) if row.isNullAt(i) => if (field.dataType == NullType) document.set(field.name, null)
      case (field, i)                    => document.set(field.name, convertToJson(row.get(i), field.dataType))
    })
    document
  }

  private def convertToJson(element: Any, elementType: DataType): Any = {
    elementType match {
      case BinaryType           => element.asInstanceOf[Array[Byte]]
      case BooleanType          => element.asInstanceOf[Boolean]
      case DateType             => element.asInstanceOf[Date].getTime
      case DoubleType           => element.asInstanceOf[Double]
      case IntegerType          => element.asInstanceOf[Int]
      case LongType             => element.asInstanceOf[Long]
      case StringType           => element.asInstanceOf[String]
      case TimestampType        => element.asInstanceOf[Timestamp].getTime
      case arrayType: ArrayType => arrayTypeToJSONArray(arrayType.elementType, element.asInstanceOf[Seq[_]])
      case mapType: MapType =>
        mapType.keyType match {
          case StringType => mapTypeToJSONObject(mapType.valueType, element.asInstanceOf[Map[String, _]])
          case _ => throw new Exception(
            s"Cannot cast $element into a Json value. MapTypes must have keys of StringType for conversion into a Document"
          )
        }
      case _ =>
        throw new Exception(s"Cannot cast $element into a Json value. $elementType has no matching Json value.")
    }
  }

  private def mapTypeToJSONObject(valueType: DataType, data: Map[String, Any]): JSONObject = {
    var jsonObject: JSONObject = new JSONObject()
    val internalData = valueType match {
      case subDocuments: StructType => data.map(kv => jsonObject.put(kv._1, rowToDocument(kv._2.asInstanceOf[Row])))
      case subArray: ArrayType      => data.map(kv => jsonObject.put(kv._1, arrayTypeToJSONArray(subArray.elementType, kv._2.asInstanceOf[Seq[Any]])))
      case _                        => data.map(kv => jsonObject.put(kv._1, convertToJson(kv._2, valueType)))
    }
    jsonObject
  }

  private def arrayTypeToJSONArray(elementType: DataType, data: Seq[Any]): JSONArray = {
    val internalData = elementType match {
      case subDocuments: StructType => data.map(x => rowToDocument(x.asInstanceOf[Row])).asJava
      case subArray: ArrayType      => data.map(x => arrayTypeToJSONArray(subArray.elementType, x.asInstanceOf[Seq[Any]])).asJava
      case _                        => data.map(x => convertToJson(x, elementType)).asJava
    }
    new JSONArray(internalData)
  }

  def documentToMap(document: Document): Map[String, AnyRef] = {
    if (document == null)
      new HashMap[String, AnyRef]
    else
      document.getHashMap.asScala.toMap
  }

}
