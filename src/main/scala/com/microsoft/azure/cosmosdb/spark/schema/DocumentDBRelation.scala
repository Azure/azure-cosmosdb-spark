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

import com.microsoft.azure.cosmosdb.spark.{DefaultSource, LoggingTrait}
import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.spark.rdd.DocumentDBRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, _}

class DocumentDBRelation(private val config: Config,
                         schemaProvided: Option[StructType] = None)(
                          @transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with LoggingTrait {

  implicit val _: Config = config

  // Take sample documents to infer the schema
  private lazy val lazySchema = {
    val sampleSize: Long = config.get[String](DocumentDBConfig.SampleSize)
      .getOrElse(DocumentDBConfig.DefaultSampleSize.toString)
      .toLong
    val samplingRatio = config.get[String](DocumentDBConfig.SamplingRatio)
      .getOrElse(DocumentDBConfig.DefaultSamplingRatio.toString)
      .toDouble

    DocumentDBRelation.lastSampleSize = sampleSize
    DocumentDBRelation.lastSamplingRatio = samplingRatio

    DocumentDBSchema(new DocumentDBRDD(sparkSession, config, Some(sampleSize)), samplingRatio).schema()
  }

  override lazy val schema: StructType = schemaProvided.getOrElse(lazySchema)

  override val sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(
                          requiredColumns: Array[String],
                          filters: Array[Filter]): RDD[Row] = {

    logDebug(s"DocumentDBRelation:buildScan, requiredColumns: ${requiredColumns.mkString(", ")}, filters: ${filters.mkString(", ")}")

    val rdd = new DocumentDBRDD(
      spark = sparkSession,
      config = config,
      requiredColumns = requiredColumns,
      filters = filters)

    DocumentDBRowConverter.asRow(DocumentDBRelation.pruneSchema(schema, requiredColumns), rdd)
  }

  override def equals(other: Any): Boolean = other match {
    case that: DocumentDBRelation =>
      schema == that.schema && config == that.config
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(schema, config)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val dfw = data.write.format(classOf[DefaultSource].toString)
    overwrite match {
      case true  => dfw.mode(SaveMode.Overwrite).save()
      case false => dfw.mode(SaveMode.ErrorIfExists).save()
    }
  }
}

object DocumentDBRelation {

  /**
    * For verification purpose
    */
  var lastSampleSize: Long = _
  var lastSamplingRatio: Double = _

  /**
    * Prune whole schema in order to fit with
    * required columns in Spark SQL statement.
    *
    * @param schema          Whole field projection schema.
    * @param requiredColumns Required fields in statement
    * @return A new pruned schema
    */
  def pruneSchema(
                   schema: StructType,
                   requiredColumns: Array[String]): StructType =
    pruneSchema(schema, requiredColumns.map(_ -> None): Array[(String, Option[Int])])


  /**
    * Prune whole schema in order to fit with
    * required columns taking in consideration nested columns (array elements) in Spark SQL statement.
    *
    * @param schema                   Whole field projection schema.
    * @param requiredColumnsWithIndex Required fields in statement including index within field for random accesses.
    * @return A new pruned schema
    */
  def pruneSchema(
                   schema: StructType,
                   requiredColumnsWithIndex: Array[(String, Option[Int])]): StructType = {

    val name2sfield: Map[String, StructField] = schema.fields.map(f => f.name -> f).toMap
    StructType(
      requiredColumnsWithIndex.flatMap {
        case (colname, None) => name2sfield.get(colname)
        case (colname, Some(idx)) => name2sfield.get(colname) collect {
          case field@StructField(name, ArrayType(et, _), nullable, _) =>
            val mdataBuilder = new MetadataBuilder
            //Non-functional area
            mdataBuilder.putLong("idx", idx.toLong)
            mdataBuilder.putString("colname", name)
            //End of non-functional area
            StructField(s"$name[$idx]", et, true, mdataBuilder.build())
        }
      }
    )
  }

}
