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
package com.microsoft.azure.cosmosdb.spark.gremlin;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DocumentDBInputRDDTest extends AbstractGremlinSparkTest {

    @Test
    public void shouldReadFromDocumentDBRDD() {
        final Configuration configuration = getBaseConfiguration();
        populateDocumentDBConfiguration(configuration);

        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal().withComputer(SparkGraphComputer.class);
        assertEquals(Long.valueOf(VERTEX_COUNT), g.V().count().next());
        assertEquals(Long.valueOf(VERTEX_COUNT * 2), g.V().outE().count().next());
        assertEquals(Long.valueOf(VERTEX_COUNT * 2), g.V().outE().inV().count().next());
        assertEquals(Long.valueOf(VERTEX_COUNT / 2 + 1), g.V().has("ModTwo", "0").count().next());
    }
}
