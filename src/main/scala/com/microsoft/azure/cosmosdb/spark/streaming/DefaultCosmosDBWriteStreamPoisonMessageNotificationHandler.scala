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
package com.microsoft.azure.cosmosdb.spark.streaming

import com.microsoft.azure.cosmosdb.{Document, ResourceResponse, RequestOptions}
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils

import java.io.{FileNotFoundException, PrintWriter, StringWriter}
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

class DefaultCosmosDBWriteStreamPoisonMessageNotificationHandler(configMap: Map[String, String])
    extends CosmosDBWriteStreamPoisonMessageNotificationHandler
    with CosmosDBLoggingTrait
    with Serializable
 {
    private val config = Config(configMap)

    private val sequenceNumber = new AtomicLong(0L)

    private val poisonMessageLocation: String = config
        .getOrElse(
            CosmosDBConfig.PoisonMessageLocation,
            String.valueOf(CosmosDBConfig.DefaultPoisonMessageLocation))
    
    private lazy val hdfsUtils: HdfsUtils = {
        HdfsUtils(configMap)
    }

    def onPoisonMessage(lastError: Throwable, document: Document) =
    {
        val sw = new StringWriter
        lastError.printStackTrace(new PrintWriter(sw))

        val callstack = sw.toString()
        val error = s"${lastError.getMessage()}${System.lineSeparator}${callstack}"
        val id = document.getId() 

        val payload = document.toJson()

        logWarning(s"POSION MESSAGE Id: ${id}, Error: ${error}, Document payload: ${payload}")

        if (this.poisonMessageLocation != "")
        {
            val prefix = s"${DateTimeFormatter.ISO_INSTANT.format(Instant.now())}_${sequenceNumber.incrementAndGet().toString()}"
            val errorFile = s"${prefix}_${id}_Error.txt"
            val payloadFile = s"${prefix}_${id}_Payload.json"   

            this.hdfsUtils.write(this.poisonMessageLocation, errorFile, error)
            this.hdfsUtils.write(this.poisonMessageLocation, payloadFile, payload)
        }
    }    
 }