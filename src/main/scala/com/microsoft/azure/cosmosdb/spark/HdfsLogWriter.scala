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

import java.io.Closeable
import java.util.{Timer, TimerTask, UUID}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.documentdb.CosmosLogWriter
import org.apache.spark.SparkEnv
import org.joda.time.Instant

import scala.collection.concurrent.TrieMap
import scala.util.Properties

private object HdfsLogWriter extends CosmosDBLoggingTrait {
  private val timerName = "hdfsLogWriter-cleanup-Timer"
  private val timer: Timer = new Timer(timerName, true)
  private val cleanupIntervalInMs = 60000
  private val writerCount = new AtomicInteger(0)
  val targetedMemoryBufferSizeInBytes = 50000000

  val lineSeparator = Properties.lineSeparator
  val logWriters = new TrieMap[String, HdfsLogWriter]

  def registerWriter(writer: HdfsLogWriter): Unit = {
    logWriters.put(writer.id, writer) match {
      case Some(existingWriter) =>
        throw new IllegalStateException(s"Already a writer '${writer.id}' registered.'")
      case None => if (writerCount.incrementAndGet() == 1) {
        startCleanupTimer()
      }
    }
  }

  def deregisterWriter(writer: HdfsLogWriter): Unit = {
    logWriters.remove(writer.loggingLocation)
  }

  private def startCleanupTimer() : Unit = {
    logInfo(s"$timerName: scheduling timer - delay: $cleanupIntervalInMs ms, period: $cleanupIntervalInMs ms")
    timer.schedule(
      new TimerTask { def run(): Unit = onCleanup() },
      cleanupIntervalInMs,
      cleanupIntervalInMs)
  }

  private def onCleanup() : Unit = {
    logInfo(s"$timerName: onCleanup")
    val snapshot = logWriters.readOnlySnapshot()
    val threshold = Instant.now().getMillis - cleanupIntervalInMs
    snapshot.foreach(writerHolder => {
      val lastFlushed = writerHolder._2.lastFlushed.get()
      if (lastFlushed > 0 && lastFlushed < threshold && writerHolder._2.hasData) {
        writerHolder._2.flush()
      }
    })
  }
}

private case class HdfsLogWriter
(
  correlationId: String,
  configMap: Map[String, String],
  loggingLocation: String
) extends CosmosLogWriter with Closeable with CosmosDBLoggingTrait {

  private[this] val inMemoryLock = ""
  val executorId: String = SparkEnv.get.executorId
  private[this] val fileId = new AtomicInteger(0)
  private[this] val sb: StringBuilder = new StringBuilder()
  private[this] lazy val hdfsUtils = new HdfsUtils(configMap, loggingLocation)
  val lastFlushed = new AtomicLong(-1)

  val id = s"${correlationId}_${executorId}_${loggingLocation}_${UUID.randomUUID()}"
  HdfsLogWriter.registerWriter(this)
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
        val fileName = s"${correlationId}_${executorId}_${this.fileId.incrementAndGet()}_${UUID.randomUUID().toString()}.log"
        logInfo(s"WriteLogFile: ${fileName} - ${content.length} bytes")
        hdfsUtils.writeLogFile(this.loggingLocation, fileName, content)
        lastFlushed.set(Instant.now().getMillis)
      }
      case None =>
    }
  }

  def hasData = {
    this.sb.length > 0
  }

  override def close(): Unit = {
    logInfo("Close")
    this.flush
    HdfsLogWriter.deregisterWriter(this)
  }
}