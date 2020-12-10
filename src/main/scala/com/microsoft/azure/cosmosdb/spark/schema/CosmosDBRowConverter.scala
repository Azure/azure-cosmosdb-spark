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

import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig.DefaultPreserveNullInWrite
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.documentdb._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.types.{DataType, DecimalType, _}
import org.json.{JSONArray, JSONObject}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

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

case class SerializationConfig(preserveNullInWrite: Boolean, convertNestedDocsToNativeJsonFormat: Boolean)

object SerializationConfig {
  def fromConfig(config: Map[String, String]) : SerializationConfig = {
    var preserveNullInWriteBoolean = CosmosDBConfig.DefaultPreserveNullInWrite;
    val preserveNullInWriteOpt = config.get(CosmosDBConfig.PreserveNullInWrite)
    if (preserveNullInWriteOpt.isDefined) {
      preserveNullInWriteBoolean = preserveNullInWriteOpt.get.toBoolean
    }

    val convertNestedDocsToNativeJsonFormat: Boolean = config.getOrElse(
      CosmosDBConfig.ConvertNestedDocsToNativeJsonFormat
      , String.valueOf(CosmosDBConfig.DefaultConvertNestedDocsToNativeJsonFormat)).toBoolean

    SerializationConfig(preserveNullInWriteBoolean, convertNestedDocsToNativeJsonFormat)
  }

  def fromConfig(config: Config) : SerializationConfig = {
      SerializationConfig(config.getOrElse(CosmosDBConfig.PreserveNullInWrite, String.valueOf(CosmosDBConfig.DefaultPreserveNullInWrite)).toBoolean
        , config.getOrElse(CosmosDBConfig.ConvertNestedDocsToNativeJsonFormat, String.valueOf(CosmosDBConfig.DefaultConvertNestedDocsToNativeJsonFormat)).toBoolean
      )
  }
}

class CosmosDBRowConverter(serializationConfig: SerializationConfig = SerializationConfig.fromConfig(Map[String, String]())) extends RowConverter[Document]
  with JsonSupport
  with Serializable
  with CosmosDBLoggingTrait {

  def asRow(schema: StructType, rdd: RDD[Document]): RDD[Row] = {
      rdd.map { record => {
        try {
            recordAsRow(record, schema)
        } catch {
          case e: Exception =>
            throw e
        }
      }
    }
  }

  def asRow(schema: StructType, array: Array[Document]): Array[Row] = {
    array.map { record =>
        recordAsRow(record, schema)
    }
  }

  def recordAsRow(
                   json: Document,
                   schema: StructType): Row = {

    val values: Seq[Any] = schema.fields.map {
      case StructField(name, et, _, mdata)
        if mdata.contains("idx") && mdata.contains("colname") =>
          val colName = mdata.getString("colname")
          val idx = mdata.getLong("idx").toInt
          documentToMap(json).get(colName).flatMap(v => Option(v)).map(toSQL(_, ArrayType(et, containsNull = true))).collect {
            case elemsList: Seq[_] if elemsList.indices contains idx => elemsList(idx)
          } orNull
      case StructField(name, dataType, _, _) =>
        if (!serializationConfig.convertNestedDocsToNativeJsonFormat || dataType != StringType) {
          documentToMap(json).get(name).flatMap(v => Option(v)).map(toSQL(_, dataType)).orNull
        } else {
          if (json.get(name) == null) {
            null
          } else {
            json.get(name).toString
          }
        }
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  def mapAsRow(
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
            if(JSONObject.NULL.equals(value)) return null

            val jsonMap: Map[String, AnyRef] = value match {
              case doc: Document => documentToMap(doc)
              case hm: util.HashMap[_, _] => hm.asInstanceOf[util.HashMap[String, AnyRef]].asScala.toMap
            }
          mapAsRow(jsonMap, struct)
        case (_, map: MapType) =>
          (value match {
            case document: Document => documentToMap(document)
            case _ => value.asInstanceOf[java.util.HashMap[String, AnyRef]].asScala.toMap
          }).map(element => (toSQL(element._1, map.keyType), toSQL(element._2, map.valueType)))
        case (_, array: ArrayType) =>
          if(!JSONObject.NULL.equals(value))
            value.asInstanceOf[java.util.ArrayList[AnyRef]].asScala.map(element => toSQL(element, array.elementType)).toArray
          else
            null
        case (_, binaryType: BinaryType) =>
          value.asInstanceOf[java.util.ArrayList[Int]].asScala.map(x => x.toByte).toArray
        case _ =>
          //Assure value is mapped to schema constrained type.
          enforceCorrectType(value, dataType)
      }
    }.orNull
  }

  private def rowTyperouterToJsonArray(element: Any, schema: StructType) = element match {
    case e: Row => rowToJSONObject(e)
    case e: InternalRow => internalRowToJSONObject(e, schema)
    case _ => throw new Exception(s"Cannot cast $element into a Json value. Struct $element has no matching Json value.")
  }

  def rowToJSONObject(row: Row): JSONObject = {
    val jsonObject: JSONObject = new JSONObject()
    if (serializationConfig.preserveNullInWrite) {
      row.schema.fields.zipWithIndex.foreach({
        case (field, i) => jsonObject.put(field.name, convertToJson(row.get(i), field.dataType, isInternalRow = false))
      })
    } else {
      row.schema.fields.zipWithIndex.foreach({
        case (field, i) if row.isNullAt(i) => if (field.dataType == NullType) jsonObject.remove(field.name)
        case (field, i) => jsonObject.put(field.name, convertToJson(row.get(i), field.dataType, isInternalRow = false))
      })
    }

    jsonObject
  }

  def internalRowToJSONObject(internalRow: InternalRow, schema: StructType): JSONObject = {
    val jsonObject: JSONObject = new JSONObject()
    if (serializationConfig.preserveNullInWrite) {
      schema.fields.zipWithIndex.foreach({
        case (field, i) => jsonObject.put(field.name, convertToJson(internalRow.get(i, field.dataType), field.dataType, isInternalRow = true))
      })
    } else {
      schema.fields.zipWithIndex.foreach({
        case (field, i) if internalRow.isNullAt(i) => if (field.dataType == NullType) jsonObject.remove(field.name)
        case (field, i) => jsonObject.put(field.name, convertToJson(internalRow.get(i, field.dataType), field.dataType, isInternalRow = true))
      })
    }
    jsonObject
  }

  private def convertToJson(element: Any, elementType: DataType, isInternalRow: Boolean): Any = {
    elementType match {
      case BinaryType           => element.asInstanceOf[Array[Byte]]
      case BooleanType          => element.asInstanceOf[Boolean]
      case DateType             => element.asInstanceOf[Date].getTime
      case DoubleType           => element.asInstanceOf[Double]
      case IntegerType          => element.asInstanceOf[Int]
      case LongType             => element.asInstanceOf[Long]
      case FloatType            => element.asInstanceOf[Float]
      case NullType             => JSONObject.NULL
      case DecimalType()        => if (element.isInstanceOf[Decimal]) {
        element.asInstanceOf[Decimal].toJavaBigDecimal
      } else if (element.isInstanceOf[java.lang.Long]) {
        new java.math.BigDecimal(element.asInstanceOf[java.lang.Long])
      } else {
        element.asInstanceOf[java.math.BigDecimal]
      }
      case StringType           =>
        if (isInternalRow) {
          new String(element.asInstanceOf[UTF8String].getBytes, "UTF-8")
        } else {
          element.asInstanceOf[String]
        }
      case TimestampType        => if (element.isInstanceOf[java.lang.Long]) {
        element.asInstanceOf[java.lang.Long]
      } else { 
        element.asInstanceOf[Timestamp].getTime
      }
      case arrayType: ArrayType => arrayTypeRouterToJsonArray(arrayType.elementType, element, isInternalRow)

      case mapType: MapType =>
        mapType.keyType match {
          case StringType =>
            if (isInternalRow) {
              // convert from UnsafeMapData to scala Map
              val unsafeMap = element.asInstanceOf[UnsafeMapData]
              val keys : Array[String] = unsafeMap.keyArray().toArray[UTF8String](StringType).map(_.toString)
              val values: Array[AnyRef] = unsafeMap.valueArray().toObjectArray(mapType.valueType)
              mapTypeToJSONObject(mapType.valueType, keys.zip(values).toMap, true)
            } else {
              mapTypeToJSONObject(mapType.valueType, element.asInstanceOf[Map[String, _]], false)
            }

          case _ => throw new Exception(
            s"Cannot cast $element into a Json value. MapTypes must have keys of StringType for conversion into a Document"
          )
        }

      case structType: StructType => rowTyperouterToJsonArray(element, structType)
      case _ =>
        throw new Exception(s"Cannot cast $element into a Json value. $elementType has no matching Json value.")
    }
  }

  private def mapTypeToJSONObject(valueType: DataType, data: Map[String, Any], isInternalRow: Boolean): JSONObject = {
    val jsonObject: JSONObject = new JSONObject()
    val internalData = valueType match {
      case subDocuments: StructType => data.map(kv => jsonObject.put(kv._1, rowTyperouterToJsonArray(kv._2, subDocuments)))
      case subArray: ArrayType      => data.map(kv => jsonObject.put(kv._1, arrayTypeRouterToJsonArray(subArray.elementType, kv._2, isInternalRow)))
      case _                        => data.map(kv => jsonObject.put(kv._1, convertToJson(kv._2, valueType, isInternalRow)))
    }
    jsonObject
  }

  private def arrayTypeRouterToJsonArray(elementType: DataType, data: Any, isInternalRow: Boolean):JSONArray = {
    data match {
      case d:Seq[_] => arrayTypeToJSONArray(elementType, d, isInternalRow)
      case d:ArrayData => arrayDataTypeToJSONArray(elementType,d, isInternalRow)
      case _ => throw new Exception(s"Cannot cast $data into a Json value. ArrayType $elementType has no matching Json value.")
    }
  }

  private def arrayTypeToJSONArray(elementType: DataType, data: Seq[Any], isInternalRow: Boolean): JSONArray = {
    val internalData = elementType match {
      case subDocuments: StructType => data.map(x => rowTyperouterToJsonArray(x, subDocuments)).asJava
      case subArray: ArrayType      => data.map(x => arrayTypeRouterToJsonArray(subArray.elementType, x, isInternalRow)).asJava
      case _                        => data.map(x => convertToJson(x, elementType, isInternalRow)).asJava
    }
    // When constructing the JSONArray, the internalData should contain JSON-compatible objects in order for the schema to be mantained.
    // Otherwise, the data will be converted into String.
    new JSONArray(internalData)
  }

  private def arrayDataTypeToJSONArray(elementType: DataType, data: ArrayData, isInternalRow: Boolean): JSONArray = {
    val listBuffer = ListBuffer.empty[Any]
    elementType match {
      case subDocuments: StructType => data.foreach(elementType, (_, x) => listBuffer.append(rowTyperouterToJsonArray(x, subDocuments)))
      case subArray: ArrayType      => data.foreach(elementType,(_,x) => listBuffer.append(arrayTypeRouterToJsonArray(subArray.elementType, x, isInternalRow)))
      case _                        => data.foreach(elementType,(_,x) => listBuffer.append(convertToJson(x, elementType, isInternalRow)))
    }
    // When constructing the JSONArray, the internalData should contain JSON-compatible objects in order for the schema to be mantained.
    // Otherwise, the data will be converted into String.
    new JSONArray(listBuffer.toList.asJava)
  }



  def documentToMap(document: Document): Map[String, AnyRef] = {
    if (document == null)
      new HashMap[String, AnyRef]
    else
      document.getHashMap.asScala.toMap
  }

}
