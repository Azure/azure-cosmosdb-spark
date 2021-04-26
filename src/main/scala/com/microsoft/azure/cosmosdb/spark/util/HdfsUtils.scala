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
package com.microsoft.azure.cosmosdb.spark.util

import java.io.{BufferedOutputStream, FileNotFoundException, OutputStream, PrintWriter, StringWriter}
import java.util

import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.collection.mutable
import java.net.URI

case class HdfsUtils(configMap: Map[String, String], changeFeedCheckpointLocation: String) extends CosmosDBLoggingTrait {
  private val fsConfig: Configuration = {
    val config = new Configuration()
    configMap.foreach(e => config.set(e._1, e._2))
    config
  }

  private val maxRetryCount = 50
  private val fs = FileSystem.get(new URI(changeFeedCheckpointLocation), fsConfig)

  def write(base: String, filePath: String, content: String): Unit = {
    val path = new Path(base + "/" + filePath)
    retry(maxRetryCount) {
      if (content != null && !content.isEmpty) {
        val os = fs.create(path)
        os.writeUTF(content)
        os.close()
      }
    }
  }

  def writeLogFile(base: String, filePath: String, content: String): Unit = {
    val path = new Path(base + "/" + filePath)
    retry(maxRetryCount) {
      val os = fs.create(path)
      val bos = new BufferedOutputStream(os)
      bos.write(content.getBytes("UTF-8"))
      bos.close()
    }
  }

  def read(base: String, filePath: String, alternateQueryName: String, collectionRid: String): String = {
    val path = new Path(base + "/" + filePath)
    read(path, base, alternateQueryName, collectionRid)
  }

  def read(path: Path, location: String, alternateQueryName: String, collectionRid: String): String = {
    try {
      retry(maxRetryCount) {
        val os = fs.open(path)
        val content = os.readUTF().replaceAll("\"", StringUtils.EMPTY)
        os.close()
        content
      }
    } catch {
      case _: Throwable =>
        // To handle transient flush exception after the write to checkpoint file
        val alternateQueryNameAlphaNum = HdfsUtils.filterFilename(alternateQueryName)
        val alternateSubPath = s"$alternateQueryNameAlphaNum/$collectionRid/${path.getName}"
        val alternatePath = s"$location/$alternateSubPath"
        if (fileExist(location, alternateSubPath)) {
          val os = fs.open(new Path(alternatePath))
          val content = os.readUTF().replaceAll("\"", StringUtils.EMPTY)
          logInfo(s"Fallback Read of checkpoint File - $alternatePath with size = ${content.trim.size}")
          content
        } else {
          StringUtils.EMPTY
        }
    }
  }

  def fileExist(base: String, filePath: String): Boolean = {
    val path = new Path(base + "/" + filePath)
    fs.exists(path)
  }

  def deleteFile(path: String): Unit = {
    fs.delete(new Path(path), true)
  }

  def listFiles(base: String, filePath: String): RemoteIterator[LocatedFileStatus] = {
    val path = new Path(base + "/" + filePath)
    try {
      fs.listFiles(path, false)
    } catch {
      case e:FileNotFoundException => null
    }
  }

  def writeChangeFeedTokenPartition(location: String,
                                    queryName: String,
                                    collectionRid: String,
                                    partitionId: String,
                                    token: String): Unit = {
    val queryNameAlphaNum = HdfsUtils.filterFilename(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid/$partitionId"

    write(location, path, token)
  }

  def readChangeFeedTokenPartition(location: String,
                                   queryName: String,
                                   alternateQueryName: String,
                                   collectionRid: String,
                                   partitionId: String): String = {
    val queryNameAlphaNum = HdfsUtils.filterFilename(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid/$partitionId"
    if (fileExist(location, path)) {
      read(location, path, alternateQueryName, collectionRid)
    } else {
      StringUtils.EMPTY
    }
  }

  def readChangeFeedToken(location: String,
                          queryName: String,
                          alternateQueryName: String,
                          collectionRid: String): java.util.HashMap[String, String] = {
    val queryNameAlphaNum = HdfsUtils.filterFilename(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid"
    val files = listFiles(location, path)
    val tokens = new util.HashMap[String, String]()
    if (files != null) {
      while (files.hasNext) {
        val file = files.next()
        val token = read(file.getPath, location, alternateQueryName, collectionRid)
        tokens.put(file.getPath.getName, token)
      }
    }
    tokens
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 => {
        Thread.sleep(100)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logError(s"Exception during executing HDFS operation with message: ${e.getMessage} and stacktrace: ${sw.toString}, retrying .. ")
        retry(n - 1)(fn)
      }
    }
  }
}

object HdfsUtils {

  /**
    * Get a map from Hadoop Configuration to use in persisting and reading from Hdfs file system
    * The Hadoop Configuration is not serializable
    * @param hadoopConfiguration  the hadoop configuration
    * @return                     a map of configuration values
    */
  def getConfigurationMap(hadoopConfiguration: Configuration): mutable.Map[String, String] = {
    val configMap = mutable.Map[String, String]()
    val iterator = hadoopConfiguration.iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      configMap += (entry.getKey -> entry.getValue)
    }
    configMap
  }

  def filterFilename(queryName: String): String = {
    queryName.replaceAll("[^0-9a-zA-Z-_]", StringUtils.EMPTY)
      .replaceAll("[\\W]", "--")
  }
}
