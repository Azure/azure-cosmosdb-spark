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


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DocumentDBOutputRDDTest extends AbstractGremlinSparkTest {

    @Test
    public void shouldWriteToDocumentDB() throws Exception {
        // Create a Tinkerpop modern graph in a file
        final Configuration tinkerGraphConfig = new BaseConfiguration();
        String tinkerGraphPath = TestHelper
                .generateTempFile(this.getClass(), "tinkergraph", ".gryo")
                .getPath();
        tinkerGraphConfig.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, tinkerGraphPath);
        tinkerGraphConfig.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        TinkerGraph tinkerGraph = TinkerGraph.open(tinkerGraphConfig);
        TinkerFactory.generateModern(tinkerGraph);
        tinkerGraph.close();

        // Load the modern graph and persist into DocumentDB
        final Configuration writeConfig = getBaseConfiguration();
        writeConfig.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, tinkerGraphPath);
        writeConfig.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        writeConfig.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, DocumentDBOutputRDD.class.getCanonicalName());
        populateDocumentDBConfiguration(writeConfig);
        writeConfig.setProperty(DocumentDBInputRDD.Constants.SPARK_DOCUMENTDB_COLLECTION, PERSISTED_COLLECTION_NAME);

        Graph graph = GraphFactory.open(writeConfig);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(Computer.compute(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();

        // Read from DocumentDB
        Configuration readConfig = getBaseConfiguration();
        populateDocumentDBConfiguration(readConfig);
        readConfig.setProperty(DocumentDBInputRDD.Constants.SPARK_DOCUMENTDB_COLLECTION, PERSISTED_COLLECTION_NAME);

        Graph readGraph = GraphFactory.open(readConfig);
        GraphTraversalSource g = readGraph.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(Long.valueOf(6), g.V().count().next());
        assertEquals(Long.valueOf(6), g.E().count().next());
    }
}
