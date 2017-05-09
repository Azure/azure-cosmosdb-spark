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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputRDD;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.spark.DocumentDBSpark$;
import com.microsoft.azure.documentdb.spark.config.Config;
import com.microsoft.azure.documentdb.spark.config.Config$;


public final class DocumentDBOutputRDD implements OutputRDD {

    static {
        InputOutputHelper.registerInputOutputPair(DocumentDBInputRDD.class, DocumentDBOutputRDD.class);
    }

    @Override
    public void writeGraphRDD(final Configuration configuration, final JavaPairRDD<Object, VertexWritable> graphRDD) {
        JavaRDD<Document> javaRDD = graphRDD
                .flatMap(t -> {
                   StarGraph.StarVertex v = t._2().get();
                   List<Document> documentList = new ArrayList<>();

                   Document d = new Document();
                   d.setId(v.id().toString());
                   d.set(DocumentDBInputRDD.Constants.LABEL_PROPERTY, v.label());
                   Map<String, List<String>> vps = new HashMap<>();
                   v.properties().forEachRemaining(p -> {
                                if (!vps.containsKey(p.label())) {
                                    vps.put(p.label(), new ArrayList<>());
                                }
                                vps.get(p.label()).add(p.value().toString());
                           });
                   for (Map.Entry<String, List<String>> entry : vps.entrySet()) {
                       JSONObject[] jsonObjArr = new JSONObject[entry.getValue().size()];
                       for (int i = 0; i < entry.getValue().size(); ++i) {
                           jsonObjArr[i] = new JSONObject(String.format("{ '%s': '%s', '%s': '%s' }",
                                   DocumentDBInputRDD.Constants.ID_PROPERTY,
                                   UUID.randomUUID().toString(),
                                   DocumentDBInputRDD.Constants.VALUE_PROPERTY,
                                   entry.getValue().get(i)));
                       }
                       d.set(entry.getKey(), jsonObjArr);
                   }

                   documentList.add(d);

                   v.edges(Direction.OUT).forEachRemaining(edge -> {
                       Document e = new Document();
                       e.setId(edge.id().toString());
                       e.set(DocumentDBInputRDD.Constants.LABEL_PROPERTY, edge.label());
                       e.set(DocumentDBInputRDD.Constants.SINK_PROPERTY, edge.inVertex().id().toString());
                       e.set(DocumentDBInputRDD.Constants.VERTEXID_PROPERTY, v.id().toString());
                       e.set(DocumentDBInputRDD.Constants.VERTEX_LABEL_PROPERTY, v.label());
                       edge.properties().forEachRemaining(p -> e.set(p.key(), p.value()));
                       documentList.add(e);
                   });

                   return documentList.iterator();
                });

        SparkContext sparkContext = graphRDD.rdd().sparkContext();
        Config writeConfig = Config$.MODULE$.apply(sparkContext.getConf());

        DocumentDBSpark$.MODULE$.save(javaRDD, writeConfig);
    }
}
