// /**
//  * The MIT License (MIT)
//  * Copyright (c) 2017 Microsoft Corporation
//  *
//  * Permission is hereby granted, free of charge, to any person obtaining a copy
//  * of this software and associated documentation files (the "Software"), to deal
//  * in the Software without restriction, including without limitation the rights
//  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  * copies of the Software, and to permit persons to whom the Software is
//  * furnished to do so, subject to the following conditions:
//  *
//  * The above copyright notice and this permission notice shall be included in all
//  * copies or substantial portions of the Software.
//  *
//  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  * SOFTWARE.
//  */
// package com.microsoft.azure.cosmosdb.spark.gremlin;

// import java.util.ArrayList;
// import java.util.Collection;
// import java.util.HashMap;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Map;

// import org.apache.commons.configuration.Configuration;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
// import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
// import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
// import org.apache.tinkerpop.gremlin.structure.T;
// import org.apache.tinkerpop.gremlin.structure.Vertex;
// import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
// import org.json.JSONObject;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.microsoft.azure.cosmosdb.spark.SparkContextFunctions$;
// import com.microsoft.azure.documentdb.Document;
// import com.microsoft.azure.cosmosdb.spark.config.Config;
// import com.microsoft.azure.cosmosdb.spark.config.Config$;
// import com.microsoft.azure.cosmosdb.spark.rdd.CosmosDBRDD;

// import scala.Option;
// import scala.Tuple2;

// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.EDGE_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.ID_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.LABEL_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.SINK_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.SINK_V_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.VERTEXID_PROPERTY;
// import static com.microsoft.azure.cosmosdb.spark.gremlin.CosmosDBInputRDD.Constants.VERTEX_ID_PROPERTY;

// public final class CosmosDBInputRDD implements InputRDD {

//     private static Logger LOGGER = LoggerFactory.getLogger(CosmosDBInputRDD.class);

//     private static CosmosDBRDD cosmosDBRDD = null;

//     static {
//         InputOutputHelper.registerInputOutputPair(CosmosDBInputRDD.class, CosmosDBOutputRDD.class);
//     }

//     @Override
//     public JavaPairRDD<Object, VertexWritable> readGraphRDD(final Configuration configuration, final JavaSparkContext sparkContext) {

//         Config readConfig = Config$.MODULE$.apply(sparkContext.getConf());

//         if (cosmosDBRDD == null) {
//             cosmosDBRDD = SparkContextFunctions$.MODULE$
//                     .apply(JavaSparkContext.toSparkContext(sparkContext))
//                     .loadFromCosmosDB(readConfig);
//             cosmosDBRDD.cache();
//         }

//         Option<Object> graphSchemaVersion = readConfig.properties().get(Constants.GRAPH_SCHEMA_VERSION);

//         if (graphSchemaVersion.isEmpty()) {
//             // Read the graph using the latest schema

//             // parsing all properties - groupBy approach
//             JavaPairRDD<String, Iterable<String>> edges = cosmosDBRDD
//                     .toJavaRDD()
//                     .filter(d -> d.get(VERTEXID_PROPERTY) != null)
//                     .mapToPair(d -> new Tuple2<>(
//                             d.getString(VERTEXID_PROPERTY),
//                             d.toJson()))
//                     .groupByKey();

//             return cosmosDBRDD
//                     .toJavaRDD()
//                     .filter(d -> d.get(VERTEXID_PROPERTY) == null)
//                     .mapToPair(d -> new Tuple2<>(d.getId(), d.toJson()))
//                     .leftOuterJoin(edges)
//                     .filter(t -> t._1() != null && t._2()._1() != null && t._2()._2() != null)
//                     .mapToPair(t -> {
//                         StarGraph graph = StarGraph.open();

//                         // create vertex from Document
//                         Document vertexDoc = new Document(t._2()._1());
//                         List<Object> properties = new ArrayList<>();
//                         properties.add(T.id);
//                         properties.add(t._1());
//                         properties.add(T.label);
//                         properties.add(vertexDoc.getString(LABEL_PROPERTY));
//                         for (Map.Entry<String, Object> entry : vertexDoc.getHashMap().entrySet()) {
//                             String key = entry.getKey();
//                             if (!key.startsWith("_") && !key.equals(LABEL_PROPERTY) && !key.equals(ID_PROPERTY)) {
//                                 Object value = entry.getValue();
//                                 if (value instanceof List<?>) {
//                                     for (Object propObj : ((List<Object>) value)) {
//                                         if (propObj instanceof HashMap<?, ?>) {
//                                             properties.add(key);
//                                             properties.add(((HashMap<String, Object>) propObj).get("_value"));
//                                         }
//                                     }
//                                 } else {
//                                     properties.add(key);
//                                     properties.add(value.getClass().toString() + value.toString());
//                                 }
//                             }
//                         }
//                         Vertex v = graph.addVertex(properties.toArray());

//                         if (t._2()._2().isPresent()) {
//                             for (String edgeJson : t._2()._2().get()) {
//                                 Document edgeDoc = new Document(edgeJson);

//                                 // add edge from document
//                                 String edgeLabel = edgeDoc.getString(LABEL_PROPERTY);
//                                 String sinkVId = edgeDoc.getString(SINK_PROPERTY);
//                                 properties = new ArrayList<>();
//                                 for (Map.Entry<String, Object> entry : edgeDoc.getHashMap().entrySet()) {
//                                     String key = entry.getKey();
//                                     if (!key.startsWith("_") && !key.equals(LABEL_PROPERTY) && !key.equals(ID_PROPERTY)) {
//                                         Object value = entry.getValue();
//                                         properties.add(key);
//                                         properties.add(value);
//                                     }
//                                 }
//                                 v.addEdge(edgeLabel, graph.addVertex(T.id, sinkVId), properties.toArray());
//                             }
//                         }

//                         return new Tuple2<>(v.id(), new VertexWritable(v));
//                     });
//         } else if (graphSchemaVersion.get().toString().equals(Constants.SPARK_DOCUMENTDB_GRAPH_SCHEMA_2017_05_01)) {
//             // parsing all properties - groupBy approach
//             JavaPairRDD<String, Iterable<String>> edges = cosmosDBRDD
//                     .toJavaRDD()
//                     .filter(d -> d.get(VERTEX_ID_PROPERTY) != null)
//                     .mapToPair(d -> new Tuple2<>(
//                             d.getString(VERTEX_ID_PROPERTY),
//                             d.toJson()))
//                     .groupByKey();

//             return cosmosDBRDD
//                     .toJavaRDD()
//                     .filter(d -> d.get(VERTEX_ID_PROPERTY) == null)
//                     .mapToPair(d -> new Tuple2<>(d.getId(), d.toJson()))
//                     .leftOuterJoin(edges)
//                     .filter(t -> t._1() != null && t._2()._1() != null && t._2()._2() != null)
//                     .mapToPair(t -> {
//                         StarGraph graph = StarGraph.open();

//                         // create vertex from Document
//                         Document vertexDoc = new Document(t._2()._1());
//                         List<Object> properties = new ArrayList<>();
//                         properties.add(T.id);
//                         properties.add(t._1());
//                         properties.add(T.label);
//                         properties.add(vertexDoc.getString(LABEL_PROPERTY));
//                         for (Map.Entry<String, Object> entry : vertexDoc.getHashMap().entrySet()) {
//                             String key = entry.getKey();
//                             if (!key.startsWith("_") && !key.equals(LABEL_PROPERTY) && !key.equals(ID_PROPERTY)) {
//                                 Object value = entry.getValue();
//                                 if (value instanceof List<?>) {
//                                     for (Object propObj : ((List<Object>) value)) {
//                                         if (propObj instanceof HashMap<?, ?>) {
//                                             properties.add(key);
//                                             properties.add(((HashMap<String, Object>) propObj).get("_value"));
//                                         }
//                                     }
//                                 } else {
//                                     properties.add(key);
//                                     properties.add(value.getClass().toString() + value.toString());
//                                 }
//                             }
//                         }
//                         Vertex v = graph.addVertex(properties.toArray());

//                         if (t._2()._2().isPresent()) {
//                             for (String edgeJson : t._2()._2().get()) {
//                                 Document edgeDoc = new Document(edgeJson);

//                                 // add edge from document
//                                 Collection<JSONObject> edgeCollection = edgeDoc.getCollection(EDGE_PROPERTY);
//                                 if (edgeCollection != null) {
//                                     // Old schema
//                                     JSONObject edgeObj = edgeCollection.iterator().next();
//                                     String edgeLabel = edgeObj.getString(LABEL_PROPERTY);
//                                     String sinkVId = edgeObj.getString(SINK_V_PROPERTY);
//                                     properties = new ArrayList<>();
//                                     Iterator<?> keyIterator = edgeObj.keys();
//                                     while (keyIterator.hasNext()) {
//                                         String key = keyIterator.next().toString();
//                                         if (!key.startsWith("_") && !key.equals(LABEL_PROPERTY) && !key.equals(ID_PROPERTY)) {
//                                             Object value = edgeObj.get(key);
//                                             properties.add(key);
//                                             properties.add(value.toString());
//                                         }
//                                     }
//                                     v.addEdge(edgeLabel, graph.addVertex(T.id, sinkVId), properties.toArray());
//                                 }


//                             }
//                         }

//                         return new Tuple2<>(v.id(), new VertexWritable(v));
//                     });
//         } else {
//             throw new UnsupportedOperationException(String.format("Unknown schema version: %s", graphSchemaVersion));
//         }
//     }

//     static class Constants {
//         public static final String VERTEX_ID_PROPERTY = "_vertex_id";
//         public static final String VERTEXID_PROPERTY = "_vertexId";
//         public static final String VERTEX_LABEL_PROPERTY = "_vertexLabel";
//         public static final String ID_PROPERTY = "id";
//         public static final String EDGE_PROPERTY = "_edge";
//         public static final String SINK_V_PROPERTY = "_sinkV";
//         public static final String SINK_PROPERTY = "_sink";
//         public static final String SINK_LABEL_PROPERTY = "_sinkLabel";
//         public static final String LABEL_PROPERTY = "label";
//         public static final String VALUE_PROPERTY = "_value";

//         public static final String SPARK_DOCUMENTDB_ENDPOINT = "spark.cosmosdb.endpoint";
//         public static final String SPARK_DOCUMENTDB_MASTERKEY = "spark.cosmosdb.masterkey";
//         public static final String SPARK_DOCUMENTDB_DATABASE = "spark.cosmosdb.database";
//         public static final String SPARK_DOCUMENTDB_COLLECTION = "spark.cosmosdb.collection";
//         public static final String SPARK_DOCUMENTDB_CONNECTIONMODE = "spark.cosmosdb.connectionMode";
//         public static final String SPARK_DOCUMENTDB_SCHEMA_SAMPLING_RATIO = "spark.cosmosdb.schema_samplingratio";
//         public static final String SPARK_DOCUMENTDB_QUERY_CUSTOM = "spark.cosmosdb.query_custom";
//         public static final String SPARK_DOCUMENTDB_GRAPH_SCHEMA_VERSION = "spark.cosmosdb.graph_schema_version";
//         public static final String GRAPH_SCHEMA_VERSION =  "graph_schema_version";
//         public static final String SPARK_DOCUMENTDB_GRAPH_SCHEMA_2017_05_01 = "2017-05-01";
//     }
// }