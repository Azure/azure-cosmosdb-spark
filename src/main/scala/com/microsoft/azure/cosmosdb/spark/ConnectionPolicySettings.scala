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

/**
  * Case class for the configuration settings used to initiate connection policies
  *
  * @param connectionMode              Represents the connection mode to be used by the client in the Azure Cosmos DB
  *                                    database service.
  *                                    Direct and Gateway connectivity modes are supported. Gateway is the default.
  *                                    Refer to
  *                                    http://azure.microsoft.com/documentation/articles/documentdb- interactions-with-resources/#connectivity-options
  *                                    for additional details.
  * @param userAgentSuffix             The value of the user-agent suffix.
  * @param maxPoolSize                 The connection pool size of the httpclient, the default is 100.
  * @param requestTimeout              The request timeout (time to wait for response from network peer) in seconds
  * @param connectionIdleTimeout       The value of the timeout for an idle connection. After that time, the
  *                                    connection will be automatically closed.
  * @param maxRetryAttemptsOnThrottled The maximum number of retries in the case where the request fails because
  *                                    the service has applied rate limiting on the client.
  * @param maxRetryWaitTimeSecs        The maximum retry time in seconds.
  * @param preferredRegions            The preferred locations for geo-replicated database accounts. For example,
  *                                    "East US" as the preferred location. If PreferredRegions is non-empty, the SDK
  *                                    will prefer to use the locations in the collection in the order they are
  *                                    specified to perform operations.
  */
private[spark] case class ConnectionPolicySettings(
                                     connectionMode: String,
                                     userAgentSuffix: String,
                                     maxPoolSize: Int,
                                     requestTimeout: Option[Int],
                                     connectionIdleTimeout: Option[Int],
                                     maxRetryAttemptsOnThrottled: Int,
                                     maxRetryWaitTimeSecs: Int,
                                     preferredRegions: Option[String])