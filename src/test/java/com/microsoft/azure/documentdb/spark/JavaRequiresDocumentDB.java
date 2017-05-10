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
package com.microsoft.azure.documentdb.spark;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

import com.microsoft.azure.documentdb.DocumentClient;

public abstract class JavaRequiresDocumentDB implements Serializable {

    private transient JavaSparkContext jsc;

    private static final DocumentDBDefaults documentDBDefaults = DocumentDBDefaults$.MODULE$.apply();

    public DocumentClient getDocumentClient() {
        return documentDBDefaults.getDocumentClient();
    }

    public SparkConf getSparkConf() {
        return getSparkConf(getCollectionName());
    }

    public SparkConf getSparkConf(final String collectionName) {
        return documentDBDefaults.getSparkConf(collectionName);
    }

    public JavaSparkContext getJavaSparkContext() {
        if (jsc != null) {
            jsc.stop();
        }
        jsc = new JavaSparkContext(new SparkContext(getSparkConf()));
        return jsc;
    }

    public JavaSparkContext getJavaSparkContext(final String collectionName) {
        if (jsc != null) {
            jsc.stop();
        }
        jsc = new JavaSparkContext(new SparkContext(getSparkConf(collectionName)));
        return jsc;
    }

    public String getDatabaseName() {
        return documentDBDefaults.DATABASE_NAME();
    }

    public String getCollectionName() {
        return this.getClass().getName();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }
}
