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
package com.microsoft.azure.cosmosdb.spark.rdd

import com.microsoft.azure.documentdb._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, Dataset};

case class JavaCosmosDBRDD(override val rdd: CosmosDBRDD) extends JavaRDD[Document](rdd) {
  /**
    * Creates a `DataFrame` inferring the schema by sampling data from CosmosDB.
    *
    * '''Note:''' Prefer [[toDS[T](beanClass:Class[T])*]] as any computations will be more efficient.
    *
    * @return a DataFrame
    */
  def toDF(): DataFrame = rdd.toDF()

  /**
    * Creates a `DataFrame` based on the schema derived from the bean class
    *
    * @param beanClass encapsulating the data from CosmosDB
    * @tparam T The bean class type to shape the data from CosmosDB into
    * @return a DataFrame
    */
  def toDF[T](beanClass: Class[T]): DataFrame = {
    rdd.toDF(beanClass)
  }


  /**
    * Creates a `Dataset` from the RDD strongly typed to the provided java bean.
    *
    * @param beanClass encapsulating the data from CosmosDB
    * @tparam T The type of the data from CosmosDB
    * @return a Dataset
    */
  def toDS[T](beanClass: Class[T]): Dataset[T] = {
    rdd.toDS(beanClass)
  }
}
