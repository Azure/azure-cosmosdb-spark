package com.microsoft.azure.cosmosdb.spark.util

import java.util

import com.microsoft.azure.cosmosdb.spark.LoggingTrait
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.collection.mutable

case class HdfsUtils(configMap: Map[String, String]) extends LoggingTrait {
  private val fsDefaultFs = "fs.defaultFS"
  private val fsConfig: Configuration = {
    val config = new Configuration()
    configMap.foreach(e => config.set(e._1, e._2))
    config
  }
  private val fs = FileSystem.get(fsConfig)

  def write(base: String, filePath: String, content: String): Unit = {
    val path = new Path(base + "/" + filePath)
    val os = fs.create(path)
    os.writeUTF(content)
    os.close()
    logInfo(s"Write $content for $path")
  }

  def read(base: String, filePath: String): String = {
    val path = new Path(base + "/" + filePath)
    read(path)
  }

  def read(path: Path): String = {
    val os = fs.open(path)
    val content = os.readUTF().replaceAll("\"", StringUtils.EMPTY)
    os.close()
    content
  }

  def fileExist(base: String, filePath: String): Boolean = {
    val path = new Path(base + "/" + filePath)
    fs.exists(path)
  }

  def listFiles(base: String, filePath: String): RemoteIterator[LocatedFileStatus] = {
    val path = new Path(base + "/" + filePath)
    fs.listFiles(path, false)
  }

  def writeChangeFeedTokenPartition(location: String,
                                    queryName: String,
                                    collectionRid: String,
                                    partitionId: String,
                                    token: String): Unit = {
    val queryNameAlphaNum = filterQueryName(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid/$partitionId"

    write(location, path, token)
  }

  def readChangeFeedTokenPartition(location: String,
                                   queryName: String,
                                   collectionRid: String,
                                   partitionId: String): String = {
    val queryNameAlphaNum = filterQueryName(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid/$partitionId"
    if (fileExist(location, path)) {
      read(location, path)
    } else {
      StringUtils.EMPTY
    }
  }

  def readChangeFeedToken(location: String,
                          queryName: String,
                          collectionRid: String): java.util.HashMap[String, String] = {
    val queryNameAlphaNum = filterQueryName(queryName)
    val path = s"$queryNameAlphaNum/$collectionRid"
    val files = listFiles(location, path)
    var tokens = new util.HashMap[String, String]()
    while (files.hasNext) {
      val file = files.next()
      val token = read(file.getPath)
      tokens.put(file.getPath.getName, token)
    }
    tokens
  }

  private def filterQueryName(queryName: String): String = {
    queryName.replaceAll("[^0-9a-zA-Z]", StringUtils.EMPTY)
  }
}
