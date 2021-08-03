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

import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.documentdb.Document
import org.apache.spark.sql.types._
import org.json.JSONObject

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

/**
  * Json - Scala object transformation support.
  * Used to convert from DBObjects to Spark SQL Row field types.
  * Disclaimer: As explained in NOTICE.md, some of this product includes
  * software developed by The Apache Software Foundation (http://www.apache.org/).
  */
trait JsonSupport extends CosmosDBLoggingTrait {

  /**
    * Tries to convert some scala value to another compatible given type
    *
    * @param value       Value to be converted
    * @param desiredType Destiny type
    * @return Converted value
    */


  protected def enforceCorrectType(value: Any, desiredType: DataType, isOpaqueJsonField: Boolean): Any =
    Option(value).map { _ =>
      desiredType match {
        case _ if value == JSONObject.NULL => null // guard when null value was inserted in document
        case StringType => toString(value, isOpaqueJsonField)
        case _ if value == "" => null // guard the non string type
        case ByteType => toByte(value)
        case BinaryType => toBinary(value)
        case ShortType => toShort(value)
        case IntegerType => toInt(value).getOrElse(null)
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case DecimalType() => toDecimal(value)
        case FloatType => toFloat(value)
        case BooleanType => value.asInstanceOf[Boolean]
        case DateType => toDate(value)
        case TimestampType => toTimestamp(value)
        case NullType => null
        case _ =>
          sys.error(s"Unsupported datatype conversion [Value: $value] of ${value.getClass}] to $desiredType]")
          value
      }
    }.orNull

  private def toBinary(value: Any): Array[Byte] = {
    value match {
      case value: Array[Byte] => value
    }
  }

  private def toByte(value: Any): Byte = {
    value match {
      case value: java.lang.Integer => value.byteValue()
      case value: java.lang.Long => value.byteValue()
      case value: java.lang.String => value.toByte
    }
  }

  private def toShort(value: Any): Short = {
    value match {
      case value: java.lang.Integer => value.toShort
      case value: java.lang.Long => value.toShort
      case value: java.lang.String => value.toShort
    }
  }

  private def toInt(value: Any): Option[Int] = {
    import scala.language.reflectiveCalls
    try {
      value match {
        case value: String => Some(value.toInt)
        case _ => Some(value.asInstanceOf[ {def toInt: Int}].toInt)
      }
    } catch {
      case e: Exception => None
    }
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
      case value: java.lang.Double => value.asInstanceOf[Double].toLong
      case value: java.lang.String => value.toLong
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
      case value: java.lang.String => value.toDouble
    }
  }

  private def toDecimal(value: Any): java.math.BigDecimal = {
    value match {
      case value: java.lang.Integer => new java.math.BigDecimal(value)
      case value: java.lang.Long => new java.math.BigDecimal(value)
      case value: java.lang.Double => new java.math.BigDecimal(value)
      case value: java.math.BigInteger => new java.math.BigDecimal(value)
      case value: java.math.BigDecimal => value
      case value: java.lang.String => new java.math.BigDecimal(value)
    }
  }

  private def toFloat(value: Any): Float = {
    value match {
      case value: java.lang.Integer => value.toFloat
      case value: java.lang.Long => value.toFloat
      case value: java.lang.Double => value.toFloat
      case value: java.lang.String => value.toFloat
    }
  }

  private def toTimestamp(value: Any): Timestamp = {
    value match {
      case value: java.util.Date => new Timestamp(value.getTime)
      case value: java.lang.Long => new Timestamp(value)
      case value: java.lang.String => Timestamp.valueOf(value)
    }
  }

  private def toDate(value: Any): Date = {
    value match {
      case value: java.util.Date => new Date(value.getTime)
      case value: java.lang.Long => new Date(value)
      case value: java.lang.String => Date.valueOf(value)
    }
  }

  private def toJsonArrayString(seq: Seq[Any], isOpaqueJsonField: Boolean): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element, isOpaqueJsonField))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any], isOpaqueJsonField: Boolean): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        val stringValue = if (value.isInstanceOf[String]) s"""\"$value\"""" else toString(value, isOpaqueJsonField)
        builder.append(s"""\"$key\":$stringValue""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any, isOpaqueJsonField: Boolean): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]], isOpaqueJsonField)
      case value: Seq[_] => toJsonArrayString(value, isOpaqueJsonField)
      case value: String => value
      case v => {
        if (Option(v).isDefined) {
          logTrace(s"toString ${value.getClass().getName} (isOpaqueJson ${isOpaqueJsonField}")
        }

        if (Option(v).isDefined &&
          isOpaqueJsonField &&
          (value.isInstanceOf[Document] || value.isInstanceOf[java.util.HashMap[String, AnyRef]])) {

          val map = (value match {
            case document: Document => documentToMap(document)
            case _ => value.asInstanceOf[java.util.HashMap[String, AnyRef]].asScala.toMap
          })
          toJsonObjectString(map, true)
        } else {
          Option(v).map(_.toString).orNull
        }
      }
    }
  }

  private def documentToMap(document: Document): Map[String, AnyRef] = {
    if (document == null)
      new HashMap[String, AnyRef]
    else
      document.getHashMap.asScala.toMap
  }

}
