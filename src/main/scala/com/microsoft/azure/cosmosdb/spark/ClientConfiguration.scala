/**
  * The MIT License (MIT)
  * Copyright (c) 2020 Microsoft Corporation
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

import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.documentdb._
import com.microsoft.azure.documentdb.internal._

import java.lang.management.ManagementFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

/**
  * Case class used as an envelope for the configuration settings of a CosmosDBConnection. All information
  * is immutable - this case class is used as the cache key when caching CosmosDBConnections in teh Executors
  * @param host                      The CosmosDB account name
  * @param authConfig                The authentication configuration
  * @param connectionPolicySettings  The configuration settings applicable when producing a connection policy
  * @param consistencyLevel          The requested consistency level
  * @param database                  The target database name
  * @param container                 The target container name
  * @param bulkConfig                The configuration settings applicable when producing bulk executors
  */
private[spark] case class ClientConfiguration(
    host: String,
    authConfig: AuthConfig,
    connectionPolicySettings: ConnectionPolicySettings,
    consistencyLevel: String,
    database: String,
    container: String,
    bulkConfig: BulkExecutorSettings,
    countLoggingPath: Option[String],
    queryLoggingPath: Option[String],
    queryLoggingCorrelationId: Option[String],
    hadoopConfig: mutable.Map[String, String]) {
  
  def getCollectionLink(): String = {
    ClientConfiguration.getCollectionLink(database, container)
  }

  def getDatabaseLink() : String = {
    ClientConfiguration.getDatabaseLink(database)
  }

  def getQueryLoggingPath(): Option[String] = {
    queryLoggingPath
  }

  def getCountLoggingPath(): Option[String] = {
    countLoggingPath
  }
}

object ClientConfiguration extends CosmosDBLoggingTrait {
  def apply(config: Config, hadoopConfig: mutable.Map[String, String]): ClientConfiguration = {
    val database : String = config.get(CosmosDBConfig.Database).get
    val collection : String = config.get(CosmosDBConfig.Collection).get      
    val authConfig : AuthConfig = validateAndCreateAuthConfig(config, database, collection)
    val connectionPolicySettings : ConnectionPolicySettings = createConnectionPolicySettings(config)
    val countLoggingPath = config.get(CosmosDBConfig.CountLoggingPath)
    val queryLoggingPath = config.get(CosmosDBConfig.QueryLoggingPath)
    val queryLoggingCorrelationId = config.get(CosmosDBConfig.QueryLoggingCorrelationId)
    val bulkExecutorSettings : BulkExecutorSettings = createBulkExecutorSettings(config)

    // Get consistency level
    val consistencyLevel = config.getOrElse(
            CosmosDBConfig.ConsistencyLevel,
            CosmosDBConfig.DefaultConsistencyLevel)

    ClientConfiguration(
      config.get(CosmosDBConfig.Endpoint).get,
      authConfig,
      connectionPolicySettings,
      consistencyLevel,
      database,
      collection,
      bulkExecutorSettings,
      countLoggingPath,
      queryLoggingPath,
      queryLoggingCorrelationId,
      hadoopConfig
    )
  }

  private def validateAndCreateAuthConfig(config: Config, database: String, collection: String) : AuthConfig = {
    val resourceToken = config.getOrElse[String](CosmosDBConfig.ResourceToken, "")
    val masterKey = config.getOrElse[String](CosmosDBConfig.Masterkey, "")

    if ((resourceToken.isEmpty && masterKey.isEmpty) ||
        (!resourceToken.isEmpty && !masterKey.isEmpty))
    {
        throw new IllegalArgumentException(
            s"Configuration options '${CosmosDBConfig.Masterkey}' and " +
            s"'${CosmosDBConfig.ResourceToken}' are mutually exclusive. " +
            s"Exactly one of them must be defined.")    
    }

    // Check if resource token exists
    if(resourceToken.isEmpty) {
      AuthConfig(masterKey, None)
    } else {
      AuthConfig(resourceToken, Some(getCollectionLink(database, collection))) 
    }
  }

  private def createBulkExecutorSettings(config: Config) : BulkExecutorSettings = {
    val pkDef: Option[String] = config.get[String](CosmosDBConfig.PartitionKeyDefinition)
    val maxMiniBatchUpdateCount: Int = config
      .getOrElse(CosmosDBConfig.MaxMiniBatchUpdateCount, CosmosDBConfig.DefaultMaxMiniBatchUpdateCount)

    BulkExecutorSettings(
      maxMiniBatchUpdateCount,
      pkDef)
  }

  private def createConnectionPolicySettings(config: Config) : ConnectionPolicySettings = {
    val connectionMode = config.getOrElse(
        CosmosDBConfig.ConnectionMode,
        CosmosDBConfig.DefaultConnectionMode)
    
    val applicationName: String = config.getOrElse[String](CosmosDBConfig.ApplicationName, "")
    val userAgentString: String = if (applicationName.isEmpty) {
      s"${Constants.userAgentSuffix} ${ManagementFactory.getRuntimeMXBean.getName}"
    } else {
      s"${Constants.userAgentSuffix} ${ManagementFactory.getRuntimeMXBean.getName} $applicationName"
    }

    val connectionRequestTimeoutInSeconds : Option[Int] = 
        config.get[String](CosmosDBConfig.ConnectionRequestTimeout) match {
            case Some(timeoutText) => Some(timeoutText.toInt)
            case None => None
        }
    val connectionIdleTimeoutInSeconds : Option[Int] = 
        config.get[String](CosmosDBConfig.ConnectionIdleTimeout) match {
            case Some(timeoutText) => Some(timeoutText.toInt)
            case None => None
        }

    val maxConnectionPoolSize : Int = config.getOrElse[String](
        CosmosDBConfig.ConnectionMaxPoolSize,
        CosmosDBConfig.DefaultMaxConnectionPoolSize.toString).toInt

    val maxRetryAttemptsOnThrottled : Int = config.getOrElse[String](
        CosmosDBConfig.QueryMaxRetryOnThrottled,
        CosmosDBConfig.DefaultQueryMaxRetryOnThrottled.toString).toInt
    
    val maxRetryWaitTimeSecs : Int = config.getOrElse[String](
        CosmosDBConfig.QueryMaxRetryWaitTimeSecs,
        CosmosDBConfig.DefaultQueryMaxRetryWaitTimeSecs.toString).toInt

    val preferredRegions : Option[String] = config.get[String](CosmosDBConfig.PreferredRegionsList)
    
    ConnectionPolicySettings(
        connectionMode,
        userAgentString,
        maxConnectionPoolSize,
        connectionRequestTimeoutInSeconds,
        connectionIdleTimeoutInSeconds,
        maxRetryAttemptsOnThrottled,
        maxRetryWaitTimeSecs,
        preferredRegions)
  }

  def getCollectionLink(database: String, collection: String): String = {
    s"${getDatabaseLink(database)}/${Paths.COLLECTIONS_PATH_SEGMENT}/$collection"
  }

  def getDatabaseLink(database: String) : String = {
    s"${Paths.DATABASES_PATH_SEGMENT}/$database"
  }
}