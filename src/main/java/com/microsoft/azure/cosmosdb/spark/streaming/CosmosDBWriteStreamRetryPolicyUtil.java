/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
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
package com.microsoft.azure.cosmosdb.spark.streaming;

import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import rx.Observable;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import scala.util.Random;

public final class CosmosDBWriteStreamRetryPolicyUtil {
    public static Observable<ResourceResponse<Document>> ProcessWithRetries(
        final Document document,
        final RequestOptions requestOptions,
        final scala.Function2<Document, RequestOptions, Observable<ResourceResponse<Document>>> task,
        final scala.Function1<Throwable, Boolean> isTransientFunc,
        final scala.Function1<String, ?> loggingAction,
        final scala.Function2<Throwable, Document, ?> onPoisonMessageAction,
        final Random rnd,
        final int maxRetries,
        final int maxRetryDelayInMs,
        final int retryUntil,
        final AtomicLong attempts)
    {
        return task.apply(document, requestOptions)
            .retryWhen(errors -> errors.<Long>flatMap(t -> {
                Long attempt = attempts.incrementAndGet();
                Boolean isTransient = isTransientFunc.apply(t);
                Integer now = Instant.now().get(ChronoField.MILLI_OF_SECOND);
                Long delay = (long)rnd.nextInt(maxRetryDelayInMs);

                loggingAction.apply("PROCESSWITHRETRIES attempt: " + attempt.toString() + ", isTransient: " + isTransient.toString() + ", Now: " + now.toString() + ", Delay: " + delay.toString());

                if (isTransient && 
                    attempt < maxRetries &&
                    now < retryUntil)
                {
                    return Observable.timer(delay, TimeUnit.MILLISECONDS);
                }
                else
                {
                    return Observable.<Long>error(t);
                }
            }))
            .onErrorResumeNext((t) -> {
                // add default handler to config
                onPoisonMessageAction.apply(t, document);
                return Observable.<ResourceResponse<Document>>empty();
            });
    }
}