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

import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources._

private [spark] object FilterConverter extends CosmosDBLoggingTrait {
  private val queryTemplate = "SELECT %s FROM c %s"
  val defaultQuery: String = String.format(queryTemplate, "*", StringUtils.EMPTY)

  def createQueryString(
                         requiredColumns: Array[String],
                         filters: Array[Filter]): String = {

    var selectClause = "*"
    //Note: for small document, the projection will transport less data but it might be slower because server
    // need to process the projection.
    //if (requiredColumns.nonEmpty)   selectClause = requiredColumns.map(x => "c." + x).mkString(",")

    var whereClause = StringUtils.EMPTY
    if (filters.nonEmpty) whereClause = s"where ${createWhereClause(filters)}"

    String.format(queryTemplate, selectClause, whereClause)
  }
  
    private def createWhereClause(filters: Array[Filter]): String = {
      filters.map {
        case EqualTo(field, value)            => s"""(c${createFieldIdentifier(field)} = ${createValueClause(value)})"""
        case EqualNullSafe(field, value)      => s"""(c${createFieldIdentifier(field)} = ${createValueClause(value)})"""
        case GreaterThan(field, value)        => s"""(c${createFieldIdentifier(field)} > ${createValueClause(value)})"""
        case GreaterThanOrEqual(field, value) => s"""(c${createFieldIdentifier(field)} >= ${createValueClause(value)})"""
        case In(field, values)                => s"""(c${createFieldIdentifier(field)} IN (${values.map(value => createValueClause(value)).mkString(",")}))"""
        case LessThan(field, value)           => s"""(c${createFieldIdentifier(field)} < ${createValueClause(value)})"""
        case LessThanOrEqual(field, value)    => s"""(c${createFieldIdentifier(field)} <= ${createValueClause(value)})"""
        case IsNull(field)                    => s"""(c${createFieldIdentifier(field)} = null)"""
        case IsNotNull(field)                 => s"""(c${createFieldIdentifier(field)} != null)"""
        case And(leftFilter, rightFilter)     => s"""(${createWhereClause(Array(leftFilter))} AND ${createWhereClause(Array(rightFilter))})"""
        case Or(leftFilter, rightFilter)      => s"""(${createWhereClause(Array(leftFilter))} OR ${createWhereClause(Array(rightFilter))})"""
        case Not(filter)                      => s"""NOT ${createWhereClause(Array(filter))}"""
        case StringStartsWith(field, value)   => s"""STARTSWITH(c${createFieldIdentifier(field)}, ${createValueClause(value)})"""
        case StringEndsWith(field, value)     => s"""ENDSWITH(c${createFieldIdentifier(field)}, ${createValueClause(value)})"""
        case StringContains(field, value)     => s"""CONTAINS(c${createFieldIdentifier(field)}, ${createValueClause(value)})"""
        case default =>
          logWarning(s"Unsupported filter $default")
          "true"
      }.mkString(" AND ")
  }

  private def createFieldIdentifier(field: String): String = {
    val parts = field.split('.')
    val identifierBuilder = StringBuilder.newBuilder

    for(part <- parts) identifierBuilder.append(s"""[\"${part}\"]""" )
    identifierBuilder.toString()
  }

  private def createValueClause(value: Any): Any = {
    value match {
      case _: String => s"'$value'"
      case _: Any => value
    }
  }
}
