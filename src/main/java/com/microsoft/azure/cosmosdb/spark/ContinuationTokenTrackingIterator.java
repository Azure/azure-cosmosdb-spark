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
package com.microsoft.azure.cosmosdb.spark;

import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.FeedResponse;
import java.util.Iterator;

public final class ContinuationTokenTrackingIterator<T extends Resource> implements Iterator<T> {
    private String currentContinuationToken;
    private final FeedResponse<T> feedResponse;
    private final Iterator<T> inner;
    private final scala.Function3<String, String, String, scala.Unit> updateBookmarkFunc;
    private final scala.Function1<String, scala.Unit> loggingAction;
    private final String partitionId;

    public ContinuationTokenTrackingIterator(
        final FeedResponse<T> feedResponse,
        final scala.Function3<String, String, String, scala.Unit> updateBookmarkFunc,
        final scala.Function1<String, scala.Unit> loggingAction,
        final String partitionId) {
        this.feedResponse  = feedResponse;
        this.inner = feedResponse.getQueryIterator();
        this.updateBookmarkFunc = updateBookmarkFunc;
        this.partitionId = partitionId;
        this.loggingAction = loggingAction;
    }

	public boolean hasNext() {
		return this.inner.hasNext();
	}

	public T next() {
        T returnValue = this.inner.next();
        String nextContinuationToken = this.feedResponse.getResponseContinuation();

        if (nextContinuationToken != null &&
            !nextContinuationToken.equals(this.currentContinuationToken))
        {
            this.updateBookmarkFunc.apply(this.currentContinuationToken, nextContinuationToken, this.partitionId);
            this.loggingAction.apply(
                String.format(
                    "Tracking progress... partitionId = '%s', continuation = '%s', new token = '%s'",
                    this.partitionId,
                    this.currentContinuationToken,
                    nextContinuationToken));
            this.currentContinuationToken = nextContinuationToken;
        }
        
        return returnValue;
	}
}