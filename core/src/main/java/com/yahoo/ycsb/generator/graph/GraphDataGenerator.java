/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this edgeFile except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE edgeFile.
 */

package com.yahoo.ycsb.generator.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteIteratorAdapter;
import com.yahoo.ycsb.generator.StoringGenerator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract class to generate {@link Graph}s and return {@link Node}s and {@link Edge}s given their ids.
 */
public abstract class GraphDataGenerator extends StoringGenerator<Graph> {

  static final String LOAD_EDGE_FILE_NAME = Edge.EDGE_IDENTIFIER + "load.json";
  static final String LOAD_NODE_FILE_NAME = Node.NODE_IDENTIFIER + "load.json";

  private static final String RUN_EDGE_FILE_NAME = Edge.EDGE_IDENTIFIER + "run.json";
  private static final String RUN_NODE_FILE_NAME = Node.NODE_IDENTIFIER + "run.json";
  private static final String CLASS_NAME = GraphDataGenerator.class.getSimpleName();

  private final Map<Long, Edge> edgeMap = new HashMap<>();
  private final Map<Long, Node> nodeMap = new HashMap<>();
  private final File edgeFile;
  private final File nodeFile;

  private long lastEdgeId;
  private long lastNodeId;
  private Gson gson;
  private Type valueType;
  private Graph lastValue = new Graph();

  GraphDataGenerator(String directory, boolean isRunPhase) throws IOException {
    GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(ByteIterator.class, new ByteIteratorAdapter());
    gson = gsonBuilder.create();

    valueType = new TypeToken<Map<String, ByteIterator>>() {
    }.getType();

    File directoryFile = new File(directory);

    if (isRunPhase) {
      nodeFile = new File(directory, RUN_NODE_FILE_NAME);
      edgeFile = new File(directory, RUN_EDGE_FILE_NAME);
    } else {
      nodeFile = new File(directory, LOAD_NODE_FILE_NAME);
      edgeFile = new File(directory, LOAD_EDGE_FILE_NAME);
    }

    if (!checkFiles(directoryFile, nodeFile, edgeFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  /**
   * Creates a {@link GraphDataRecorder} or a {@link GraphDataRecreator} depending on the given values.
   *
   * @param directory  which contains the recorded data or where the data will be recorded to.
   * @param isRunPhase tells the current execution phase (load or run).
   * @param properties passed to the {@link GraphDataRecorder} constructor to read the needed properties.
   * @return a subclass of the {@link GraphDataGenerator}.
   * @throws IOException if an I/O exception occurs.
   */
  public static GraphDataGenerator create(String directory,
                                          boolean isRunPhase,
                                          Properties properties) throws IOException {
    GraphDataGenerator graphGenerator;
    File loadNodeFile = new File(directory, LOAD_NODE_FILE_NAME);
    File loadEdgeFile = new File(directory, LOAD_EDGE_FILE_NAME);

    boolean loadDataPresent = checkDataPresentAndCleanIfSomeMissing(CLASS_NAME,
        loadNodeFile,
        loadEdgeFile);
    boolean runDataPresent = checkDataPresentAndCleanIfSomeMissing(CLASS_NAME,
        new File(directory, RUN_NODE_FILE_NAME),
        new File(directory, RUN_EDGE_FILE_NAME));

//  load runData isRun return
//  0    0       0     recorder
//  0    0       1     recorder
//  0    1       0     recorder
//  0    1       1     recreator ==> KV-Diagram leads to recreate =
//  1    0       0     recreator                         isRunPhase && runDataPresent || !isRunPhase && loadDataPresent
//  1    0       1     recorder
//  1    1       0     recreator
//  1    1       1     recreator
    if (isRunPhase && runDataPresent || !isRunPhase && loadDataPresent) {
      System.out.println(CLASS_NAME + " creating RECREATOR.");
      graphGenerator = new GraphDataRecreator(directory, isRunPhase);
    } else {
      System.out.println(CLASS_NAME + " creating RECORDER.");
      graphGenerator = new GraphDataRecorder(directory, isRunPhase, properties);
    }

    if (isRunPhase && loadNodeFile.exists() && loadEdgeFile.exists()) {
      graphGenerator.prepareMaps(loadNodeFile);
    }

    return graphGenerator;
  }

  @Override
  public final Graph nextValue() {
    Graph graph = new Graph();
    try {
      graph = createNextValue();
    } catch (IOException e) {
      e.printStackTrace();
    }

    storeGraphComponents(graph);

    return graph;
  }

  @Override
  public final Graph lastValue() {
    return lastValue;
  }

  public long getLastNodeId() {
    return lastNodeId;
  }

  public long getLastEdgeId() {
    return lastEdgeId;
  }

  /**
   * @param key id of the {@link Node} if already generated via nextValue().
   * @return a {@link Node} or null, if not generated jet.
   */
  public Node getNode(long key) {
    return nodeMap.get(key);
  }

  /**
   * @param key id of the {@link Edge} if already generated via nextValue().
   * @return a {@link Edge} or null, if not generated jet.
   */
  public Edge getEdge(long key) {
    return edgeMap.get(key);
  }

  Gson getGson() {
    return gson;
  }

  Type getValueType() {
    return valueType;
  }

  Graph getLastValue() {
    return lastValue;
  }

  void setLastValue(Graph graph) {
    this.lastValue = graph;
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getNodeFile() {
    return nodeFile;
  }

  private void storeGraphComponents(Graph graph) {
    for (Edge edge : graph.getEdges()) {
      if (!edgeMap.containsKey(edge.getId())) {
        edgeMap.put(edge.getId(), edge);
        lastEdgeId = lastEdgeId < edge.getId() ? edge.getId() : lastEdgeId;
      }
    }

    for (Node node : graph.getNodes()) {
      if (!nodeMap.containsKey(node.getId())) {
        nodeMap.put(node.getId(), node);
        lastNodeId = lastNodeId < node.getId() ? node.getId() : lastNodeId;
      }
    }
  }

  private int getLastId(File file) throws IOException {
    List<String> lines = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));
    String lastEntry = lines.get(lines.size() - 1);

    Map<String, ByteIterator> values = getGson().fromJson(new JsonReader(new StringReader(lastEntry)), getValueType());

    return Integer.parseInt(values.get(Node.ID_IDENTIFIER).toString());
  }

  final void prepareMaps(File loadNodeFile) throws IOException {
    List<Graph> graphs = getGraphs(getLastId(loadNodeFile));

    for (Graph graph : graphs) {
      storeGraphComponents(graph);
    }
  }

  /**
   * Load the values of the {@link Node}s and {@link Edge}s from the load phase into their containers.
   *
   * @param numberOfGraphs to load.
   */
  abstract List<Graph> getGraphs(int numberOfGraphs);

  /**
   * @return the next generated value.
   * @throws IOException if an I/O exception occurs.
   */
  abstract Graph createNextValue() throws IOException;
}