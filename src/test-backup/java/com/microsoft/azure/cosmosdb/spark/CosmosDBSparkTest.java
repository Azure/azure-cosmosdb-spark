/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.spark;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark;
import com.microsoft.azure.cosmosdb.spark.rdd.JavaCosmosDBRDD;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class CosmosDBSparkTest extends JavaRequiresCosmosDB {

    List<Document> counters = asList(new Document("{counter: 0}"), new Document("{counter: 1}"),
            new Document("{counter: 2}"));

    @Test
    public void shouldBeCreatableFromTheSparkContext() {
        JavaSparkContext jsc = getJavaSparkContext();

        CosmosDBSpark.save(jsc.parallelize(counters));
        JavaCosmosDBRDD cosmosDBRDD = CosmosDBSpark.load(jsc);

        assertEquals(cosmosDBRDD.count(), 3);

        List<Integer> counters = cosmosDBRDD.map(new Function<Document, Integer>() {
            public Integer call(final Document x) throws Exception {
                return x.getInt("counter");
            }
        }).collect();
        assertEquals(counters, asList(0, 1, 2));

    }
}
