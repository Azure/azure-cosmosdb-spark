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
package com.microsoft.azure.cosmosdb

import com.microsoft.azure.cosmosdb.spark.DefaultHelper.DefaultsTo
import com.microsoft.azure.cosmosdb.spark.rdd.DocumentRDDFunctions
import com.microsoft.azure.documentdb.Document
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Whole DocumentDB helpers.
  */
package object spark {
  /**
    * :: DeveloperApi ::
    *
    * Helper to implicitly add DocumentDB based functions to a SparkContext
    *
    * @param sc the current SparkContext
    * @return the DocumentDB based Spark Context
    */
  @DeveloperApi
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = SparkContextFunctions(sc)

  /**
    * :: DeveloperApi ::
    *
    * Helper to implicitly add DocumentDB based functions to a SparkContext
    *
    * @param rdd the RDD to save to DocumentDB
    * @return the DocumentDB based Spark Context
    */
  @DeveloperApi
  implicit def toDocumentRDDFunctions[D](rdd: RDD[D])(implicit e: D DefaultsTo Document, ct: ClassTag[D]):
    DocumentRDDFunctions[D] = DocumentRDDFunctions(rdd)
}
