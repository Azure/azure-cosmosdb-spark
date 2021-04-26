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

import java.io.{BufferedOutputStream, Closeable}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.documentdb.CosmosLogWriter
import org.apache.spark.SparkEnv

import scala.util.Properties

private object HdfsLogWriter {
  val targetedMemoryBufferSizeInBytes = 50000000

  val lineSeparator = Properties.lineSeparator
}

private case class HdfsLogWriter
(
  correlationId: String,
  configMap: Map[String, String],
  loggingLocation: String
) extends CosmosLogWriter with Closeable with CosmosDBLoggingTrait {

  private[this] val inMemoryLock = ""
  private[this] val executorId: String = SparkEnv.get.executorId
  private[this] val fileId = new AtomicInteger(0)
  private[this] val sb: StringBuilder = new StringBuilder()
  private[this] lazy val hdfsUtils = new HdfsUtils(configMap, loggingLocation)

  logInfo("HdfsBulkLogWriter instantiated.")

  override def writeLine(line: String): Unit = {
    if (line != null) {
      val prettyLine = line.filter(_ >= ' ') + HdfsLogWriter.lineSeparator
      logDebug(s"PrettyLine: $prettyLine")
      this.inMemoryLock.synchronized {
        if (sb.length + prettyLine.length >= HdfsLogWriter.targetedMemoryBufferSizeInBytes) {
          this.flush()
        }

        this.sb.append(prettyLine)
      }
    }
  }

  override def flush(): Unit = {
    logInfo(s"Flush: ${sb.size}")
    var contentToFlush: Option[String] = None
    this.inMemoryLock.synchronized {
      if (this.sb.size > 0) {
        contentToFlush = Some(this.sb.toString())
        this.sb.clear()
      }
    }

    contentToFlush match {
      case Some(content) => {
          val fileName = s"${correlationId}_${executorId}_${this.fileId.incrementAndGet()}.log"
          logInfo(s"WriteLogFile: ${fileName} - ${content.length} bytes")
          hdfsUtils.writeLogFile(this.loggingLocation, fileName, content)
      }
      case None =>
    }
  }

  override def close(): Unit = {
    logInfo("Close")
    this.flush
  }
}
