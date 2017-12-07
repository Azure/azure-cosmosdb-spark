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

import org.apache.spark.sql.types._

import scala.util.parsing.json.JSONObject

/**
  * Json - Scala object transformation support.
  * Used to convert from DBObjects to Spark SQL Row field types.
  * Disclaimer: As explained in NOTICE.md, some of this product includes
  * software developed by The Apache Software Foundation (http://www.apache.org/).
  */
trait JsonSupport {

  /**
    * Tries to convert some scala value to another compatible given type
    *
    * @param value       Value to be converted
    * @param desiredType Destiny type
    * @return Converted value
    */


  protected def enforceCorrectType(value: Any, desiredType: DataType): Any =
    Option(value).map { _ =>
      desiredType match {
        case StringType => toString(value)
        case _ if value == "" => null // guard the non string type
        case ByteType => toByte(value).getOrElse(null)
        case BinaryType => toBinary(value).getOrElse(null)
        case ShortType => toShort(value).getOrElse(null)
        case IntegerType => toInt(value).getOrElse(null)
        case LongType => toLong(value).getOrElse(null)
        case DoubleType => toDouble(value).getOrElse(null)
        case DecimalType() => toDecimal(value).getOrElse(null)
        case FloatType => toFloat(value).getOrElse(null)
        case BooleanType => value.asInstanceOf[Boolean]
        case DateType => toDate(value).getOrElse(null)
        case TimestampType => toTimestamp(value).getOrElse(null)
        case NullType => null
        case _ =>
          sys.error(s"Unsupported datatype conversion [Value: $value] of ${value.getClass}] to $desiredType]")
          value
      }
    }.getOrElse(null)

  private def toBinary(value: Any): Option[Array[Byte]] = {
    value match {
      case value: Array[Byte] => Some(value)
      case _ => None
    }
  }

  private def toByte(value: Any): Option[Byte] = {
    value match {
      case value: java.lang.Integer => Some(value.byteValue())
      case value: java.lang.Long => Some(value.byteValue())
      case value: String => Some(value.toByte)
      case _ => None
    }
  }

  private def toShort(value: Any): Option[Short] = {
    value match {
      case value: java.lang.Integer => Some(value.toShort)
      case value: java.lang.Long => Some(value.toShort)
      case value: String => Some(value.toShort)
      case _ => None
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

  private def toLong(value: Any): Option[Long] = {
    value match {
      case value: java.lang.Integer => Some(value.asInstanceOf[Int].toLong)
      case value: java.lang.Long => Some(value.asInstanceOf[Long])
      case value: java.lang.Double => Some(value.asInstanceOf[Double].toLong)
      case value: String => Some(value.toLong)
      case _ => None
    }
  }

  private def toDouble(value: Any): Option[Double] = {
    value match {
      case value: java.lang.Integer => Some(value.asInstanceOf[Int].toDouble)
      case value: java.lang.Long => Some(value.asInstanceOf[Long].toDouble)
      case value: java.lang.Double => Some(value.asInstanceOf[Double])
      case value: String => Some(value.toDouble)
      case _ => None
    }
  }

  private def toDecimal(value: Any): Option[java.math.BigDecimal] = {
    value match {
      case value: java.lang.Integer => Some(new java.math.BigDecimal(value))
      case value: java.lang.Long => Some(new java.math.BigDecimal(value))
      case value: java.lang.Double => Some(new java.math.BigDecimal(value))
      case value: java.math.BigInteger => Some(new java.math.BigDecimal(value))
      case value: java.math.BigDecimal => Some(value)
      case _ => None
    }
  }

  private def toFloat(value: Any): Option[Float] = {
    value match {
      case value: java.lang.Integer => Some(value.toFloat)
      case value: java.lang.Long => Some(value.toFloat)
      case value: java.lang.Double => Some(value.toFloat)
      case value: String => Some(value.toFloat)
      case _ => None
    }
  }

  private def toTimestamp(value: Any): Option[Timestamp] = {
    value match {
      case value: java.util.Date => Some(new Timestamp(value.getTime))
      case value: java.lang.Long => Some(new Timestamp(value))
      case _ => None
    }
  }

  private def toDate(value: Any): Option[Date] = {
    value match {
      case value: java.util.Date => Some(new Date(value.getTime))
      case value: java.lang.Long => Some(new Date(value))
      case _ => None
    }
  }

  private def toJsonArrayString(seq: Seq[Any]): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any]): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        val stringValue = if (value.isInstanceOf[String]) s"""\"$value\"""" else toString(value)
        builder.append(s"""\"$key\":$stringValue""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]])
      case value: Seq[_] => toJsonArrayString(value)
      case v => Option(v).map(_.toString).orNull
    }
  }

}
