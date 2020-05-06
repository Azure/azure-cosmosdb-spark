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
package com.microsoft.azure.cosmosdb.spark.config

import com.microsoft.azure.cosmosdb.spark.config.Config.Property
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  * Abstract config builder, used to set a bunch of properties a build
  * a config object from them.
  *
  * @param properties Map of any-type properties.
  * @tparam Builder Current Builder implementation type.
  */
abstract class ConfigBuilder[Builder <: ConfigBuilder[Builder]](
                                                                 val properties: Map[Property, Any] = Map()) extends Serializable {
  builder =>

  /**
    * Required properties to build a CosmosDB config object.
    * At build time, if these properties are not set, an assert
    * exception will be thrown.
    */
  val requiredProperties: List[Property]

  /**
    * Instantiate a brand new Builder from given properties map
    *
    * @param props Map of any-type properties.
    * @return The new builder
    */
  def apply(props: Map[Property, Any]): Builder

  /**
    * Set (override if exists) a single property value given a new one.
    *
    * @param property Property to be set
    * @param value    New value for given property
    * @tparam T Property type
    * @return A new builder that includes new value of the specified property
    */
  def set[T](property: Property, value: T): Builder =
    apply(properties + (property -> value))

  /**
    * Build the config object from current builder properties.
    *
    * @return The CosmosDB configuration object.
    */
  def build(): Config = new Config {

    val properties: Map[Property, Any] = builder.properties.map { case (k, v) => k.toLowerCase -> v }
    val reqProperties: List[Property] = requiredProperties.map(_.toLowerCase)

    if (get(CosmosDBConfig.ReadChangeFeed).getOrElse(CosmosDBConfig.DefaultReadChangeFeed.toString).toBoolean ||
      get(CosmosDBConfig.IncrementalView).getOrElse(CosmosDBConfig.DefaultIncrementalView.toString).toBoolean) {
      require(
        List(CosmosDBConfig.ChangeFeedQueryName).forall(properties.isDefinedAt),
        s"${CosmosDBConfig.ChangeFeedQueryName} property is required when reading change feed"
      )
    }

    /**
      * Compare if two Configs have the same properties.
      *
      * @param other Object to compare
      * @return Boolean
      */
    override def equals(other: Any): Boolean = other match {
      case that: Config =>
        properties == that.properties
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(properties)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

  }

}

/**
  * CosmosDB standard configuration object
  */
trait Config extends Serializable {


  /**
    * Contained properties in configuration object
    */
  val properties: Map[Property, Any]

  def asOptions: collection.Map[String, String] = {
    properties.map { case (x, v) => x -> v.toString() }
  }

  /** Returns the value associated with a key, or a default value if the key is not contained in the configuration object.
    *
    * @param   key     Desired property.
    * @param   default Value in case no binding for `key` is found in the map.
    * @tparam  T Result type of the default computation.
    * @return the value associated with `key` if it exists,
    *         otherwise the result of the `default` computation.
    */
  def getOrElse[T](key: Property, default: => T): T = properties.get(key) match {
    case Some(v) => v.asInstanceOf[T]
    case None => default
  }

  /**
    * Gets specified property from current configuration object
    *
    * @param property Desired property
    * @tparam T Property expected value type.
    * @return An optional value of expected type
    */
  def get[T: ClassTag](property: Property): Option[T] =
    properties.get(property.toLowerCase).map(_.asInstanceOf[T])

  /**
    * Gets specified property from current configuration object.
    * It will fail if property is not previously set.
    *
    * @param property Desired property
    * @tparam T Property expected value type
    * @return Expected type value
    */
  def apply[T: ClassTag](property: Property): T = {
    get[T](property).get
  }
}

object Config {

  val configPrefix = "spark.cosmosdb."

  type Property = String

  /**
    * Defines how to act in case any parameter is not set
    *
    * @param key Key that couldn't be obtained
    * @tparam T Expected type (used to fit in 'getOrElse' cases).
    * @return Throws an IllegalStateException.
    */
  def notFound[T](key: String): T =
    throw new IllegalStateException(s"Parameter $key not specified")

  /**
    * Create a configuration from the `sparkContext`
    *
    * Uses the prefixed properties that are set in the Spark configuration to create the config.
    *
    * @see [[configPrefix]]
    * @param sparkContext the spark context
    * @return the configuration
    */
  def apply(sparkContext: SparkContext): Config = apply(sparkContext.getConf)

  /**
    * Create a configuration from the `sqlContext`
    *
    * Uses the prefixed properties that are set in the Spark configuration to create the config.
    *
    * @see [[configPrefix]]
    * @param sparkSession the SparkSession
    * @return the configuration
    */
  def apply(sparkSession: SparkSession): Config = apply(sparkSession.sparkContext.getConf)

  /**
    * Create a configuration from the `sparkConf`
    *
    * Uses the prefixed properties that are set in the Spark configuration to create the config.
    *
    * @see [[configPrefix]]
    * @param sparkConf the spark configuration
    * @return the configuration
    */
  def apply(sparkConf: SparkConf): Config = apply(sparkConf, Map.empty[String, String])

  /**
    * Create a configuration from the `sparkConf`
    *
    * Uses the prefixed properties that are set in the Spark configuration to create the config.
    *
    * @see [[configPrefix]]
    * @param sparkConf the spark configuration
    * @param options   overloaded parameters
    * @return the configuration
    */
  def apply(sparkConf: SparkConf, options: collection.Map[String, String]): Config =
    apply(getOptionsFromConf(sparkConf) ++ stripPrefix(options))

  /**
    * Create a configuration from the values in the `Map`
    *
    * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
    *
    * @param options a map of properties and their string values
    * @return the configuration
    */
  def apply(options: collection.Map[String, String]): Config = {
    apply(options, None)
  }

  /**
    * Create a configuration from the values in the `Map`, using the optional default configuration for any default values.
    *
    * '''Note:''' Values in the map do not need to be prefixed with the [[configPrefix]].
    *
    * @param options a map of properties and their string values
    * @param default the optional default configuration, used for determining the default values for the properties
    * @return the configuration
    */
  def apply(options: collection.Map[String, String], default: Option[Config]): Config = {
    var combine = options ++ {
      default match {
        case Some(value) => value.asOptions
        case None => Map.empty[String, String]
      }
    }

    if (!combine.contains(CosmosDBConfig.Endpoint) && !combine.contains(CosmosDBConfig.Masterkey)) {
      val endpoint = System.getenv(CosmosDBConfig.EndpointEnvVarName)
      val key = System.getenv(CosmosDBConfig.KeyEnvVarname)

      if (!isEmpty(endpoint) && !isEmpty(key)) {
        LoggerFactory.getLogger("Setting cosmos credentials from env variables")
        combine += (CosmosDBConfig.Endpoint -> endpoint)
        combine += (CosmosDBConfig.Masterkey -> key)
      }
    }

    val builder = CosmosDBConfigBuilder(combine.asInstanceOf[Map[String, Any]])

    builder.build()
  }

  private def isEmpty(value: String): Boolean =
    value == null || value.isEmpty


  /**
    * Strip the prefix from options
    *
    * @param options options that may contain the prefix
    * @return prefixLess options
    */
  def stripPrefix(options: collection.Map[String, String]): collection.Map[String, String] =
    options.map(kv => (kv._1.toLowerCase.stripPrefix(configPrefix), kv._2))

  /**
    * Gets an options map from the `SparkConf`
    *
    * @param sparkConf the SparkConf
    * @return the options
    */
  def getOptionsFromConf(sparkConf: SparkConf): collection.Map[String, String] =
    stripPrefix(sparkConf.getAll.filter(_._1.startsWith(configPrefix)).toMap)

  protected def getInt(newValue: Option[String], existingValue: Option[Int] = None, defaultValue: Int): Int = {
    newValue match {
      case Some(value) => value.toInt
      case None => existingValue.getOrElse(defaultValue)
    }
  }
}
