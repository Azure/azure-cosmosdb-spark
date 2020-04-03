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

import java.io.{FileNotFoundException, PrintWriter, StringWriter}

import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}

class NoRetriesCosmosDBWriteStreamRetryPolicyConfig
 extends CosmosDBWriteStreamRetryPolicyConfig
 with CosmosDBLoggingTrait
 with Serializable
{
    def isTransient(t: Throwable) : Boolean = {
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        logError(s"NON-TRANSIENT error: ${t.getMessage}, CallStack: ${sw.toString}")
        false
    }

    def getMaxTransientRetryCount() : Int = {
        0
    }

    def getMaxTransientRetryDurationInMs() : Int = {
        0
    }

    def getMaxTransientRetryDelayInMs() : Int = {
        0
    }
}