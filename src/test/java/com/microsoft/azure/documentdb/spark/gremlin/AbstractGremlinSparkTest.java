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
package com.microsoft.azure.documentdb.spark.gremlin;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.spark.DocumentDBDefaults;

public abstract class AbstractGremlinSparkTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinSparkTest.class);

    static final String DATABASE_NAME = "graphDatabase";
    static final String COLLECTION_NAME = "graphCollection";
    static final int VERTEX_COUNT = 5;
    private static DocumentClient documentClient;

    @After
    @Before
    public void setupTest() {
        Spark.close();
        HadoopPools.close();
        KryoShimServiceLoader.close();
        logger.info("SparkContext has been closed for " + this.getClass().getCanonicalName() + "-setupTest");
    }

    @BeforeClass
    public static void setUpDocumentDB() throws DocumentClientException {
        DocumentDBDefaults documentDBDefaults = DocumentDBDefaults.apply();
        documentClient = new DocumentClient(documentDBDefaults.EMULATOR_ENDPOINT(),
                documentDBDefaults.EMULATOR_MASTERKEY(),
                new ConnectionPolicy(),
                ConsistencyLevel.Session);

        deleteData();
        populateData();
    }

    @AfterClass
    public static void cleanUpDocumentDB() {
        deleteData();
    }

    private static void populateData() throws DocumentClientException {
        Database newDb = new Database();
        newDb.setId(DATABASE_NAME);
        Database createdDb = documentClient.createDatabase(newDb, null).getResource();

        DocumentCollection collection = new DocumentCollection();
        collection.setId(COLLECTION_NAME);
        DocumentCollection createdCollection = documentClient.createCollection(createdDb.getSelfLink(), collection, null).getResource();

        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < VERTEX_COUNT; ++i) {
            documents.add(createVertexDocument(i));
            documents.add(createEdgeDocument(i, (i + 1) % VERTEX_COUNT));
            documents.add(createEdgeDocument((i - 2 + VERTEX_COUNT) % VERTEX_COUNT, i));
        }
        for (Document d : documents) {
            documentClient.createDocument(createdCollection.getSelfLink(), d, null, true);
        }
    }

    private static void deleteData() {
        try {
            documentClient.deleteDatabase("dbs/" + DATABASE_NAME, null);
        } catch (DocumentClientException ignored) {
        }
    }

    private static Document createVertexDocument(int documentId) {
        Document vDoc = new Document();
        vDoc.setId(String.valueOf(documentId));
        vDoc.set("ModTwo", new JSONObject[] { new JSONObject(String.format("{'id': '%s', '_value': '%s'}", UUID.randomUUID().toString(), documentId % 2)) });
        vDoc.set(DocumentDBInputRDD.Constants.LABEL_PROPERTY, String.format("vertex%d", documentId));
        return vDoc;
    }

    private static Document createEdgeDocument(int sourceId, int sinkId) {
        Document vEdge = new Document();
        vEdge.setId(UUID.randomUUID().toString());
        vEdge.set(DocumentDBInputRDD.Constants.LABEL_PROPERTY, String.format("edge%d-%d", sourceId, sinkId));
        vEdge.set(DocumentDBInputRDD.Constants.SINK_PROPERTY, String.valueOf(sinkId));
        vEdge.set(DocumentDBInputRDD.Constants.VERTEXID_PROPERTY, String.valueOf(sourceId));
        return vEdge;
    }

    protected Configuration getBaseConfiguration() {
        final BaseConfiguration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", KryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, DocumentDBInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(this.getClass(), "AbstractSparkTest"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        return configuration;
    }
}
