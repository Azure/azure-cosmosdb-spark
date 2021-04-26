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

import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import com.microsoft.azure.documentdb.{Document, PartitionKeyDefinition}
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

private[spark] object LoggingIterator {
  def createLoggingIterator[D: ClassTag]
  (
    inner: Iterator[D],
    logger: IteratorLogger,
    partitionKeyDefinition: PartitionKeyDefinition,
    rootPropertyToSave: Option[String],
    cosmosDBRowConverter: CosmosDBRowConverter): Iterator[D] = {

    inner.map(input => {

      try {
        val document: Document = input match {
          case doc: Document => doc
          case row: Row =>
            if (rootPropertyToSave.isDefined) {
              new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
            } else {
              new Document(cosmosDBRowConverter.rowToJSONObject(row).toString())
            }
          case any => new Document(any.toString)
        }

        logger.onIteratorNext(document, partitionKeyDefinition)
        input
      } catch {
        case t: Throwable => {
          logger.logError("Failure converting RDD item", t)
          throw t
        }
      }
    })
  }

  def createLoggingAndConvertingIterator[D: ClassTag]
  (
    inner: Iterator[D],
    logger: Option[IteratorLogger],
    partitionKeyDefinition: PartitionKeyDefinition,
    rootPropertyToSave: Option[String],
    cosmosDBRowConverter: CosmosDBRowConverter): Iterator[Document] = {

    inner.map(input => {

      try {
        val document: Document = input match {
          case doc: Document => doc
          case row: Row =>
            if (rootPropertyToSave.isDefined) {
              new Document(row.getString(row.fieldIndex(rootPropertyToSave.get)))
            } else {
              new Document(cosmosDBRowConverter.rowToJSONObject(row).toString())
            }
          case any => new Document(any.toString)
        }

        if (logger.isDefined) {
          logger.get.onIteratorNext(document, partitionKeyDefinition)
        }
        document
      } catch {
        case t: Throwable => {
          if (logger.isDefined) {
            logger.get.logError("Failure converting RDD item", t)
          }
          throw t
        }
      }
    })
  }
}


