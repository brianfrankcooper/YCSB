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
import com.yahoo.ycsb.generator.StoringGenerator;
import com.yahoo.ycsb.workloads.GraphWorkload;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract class to generate {@link Graph}s and return {@link Node}s and {@link Edge}s given their ids.
 */
public abstract class GraphDataGenerator extends StoringGenerator<Graph> {

  static final String PRODUCTS_PER_ORDER_PROPERTY = "productsperorder";
  static final int PRODUCTS_PER_ORDER_DEFAULT = 10;
  static final String COMPONENTS_PER_PRODUCT_PROPERTY = "componentsperproduct";
  static final int COMPONENTS_PER_PRODUCT_DEFAULT = 10;
  static final String TEST_PARAMETER_COUNT_PROPERTY = "testparametercount";
  static final int TEST_PARAMETER_COUNT_DEFAULT = 128;

  private static final String LOAD_GRAPH_FILE_NAME = Graph.GRAPH_IDENTIFIER + "load.json";
  private static final String RUN_GRAPH_FILE_NAME = Graph.GRAPH_IDENTIFIER + "run.json";
  private static final String CLASS_NAME = GraphDataGenerator.class.getSimpleName();

  private final int productsPerOrder;
  private final int componentsPerProduct;
  private final int testParameterCount;
  private final boolean onlyCreateNodes;

  private final Map<Long, Edge> edgeMap = new HashMap<>();
  private final Map<Long, Node> nodeMap = new HashMap<>();
  private final File graphFile;
  private long lastEdgeId;
  private long lastNodeId;
  private Gson gson;
  private Type valueType;
  private Graph lastValue = new Graph();

  GraphDataGenerator(String directory, boolean isRunPhase, Properties properties) throws IOException {
    gson = new GsonBuilder().registerTypeAdapter(Graph.class, new GraphAdapter()).create();

    valueType = new TypeToken<Graph>() {
    }.getType();

    File directoryFile = new File(directory);

    if (isRunPhase) {
      graphFile = getRunFile(directory);
    } else {
      graphFile = getLoadFile(directory);
    }

    if (!checkFiles(directoryFile, graphFile)) {
      throw new IOException(getExceptionMessage());
    }

    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_PROPERTY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT)));
    componentsPerProduct = Integer.valueOf(properties.getProperty(COMPONENTS_PER_PRODUCT_PROPERTY,
        String.valueOf(COMPONENTS_PER_PRODUCT_DEFAULT)));
    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_PROPERTY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT)));
    onlyCreateNodes = Boolean.parseBoolean(properties.getProperty(GraphWorkload.ONLY_WORK_WITH_NODES_PROPERTY,
        GraphWorkload.ONLY_WORK_WITH_NODES_DEFAUL));
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
    File loadGraphFile = getLoadFile(directory);

    boolean loadDataPresent = checkDataPresentAndCleanIfSomeMissing(CLASS_NAME, loadGraphFile);
    boolean runDataPresent = checkDataPresentAndCleanIfSomeMissing(CLASS_NAME, getRunFile(directory));

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
      graphGenerator = new GraphDataRecreator(directory, isRunPhase, properties);
    } else {
      System.out.println(CLASS_NAME + " creating RECORDER.");
      graphGenerator = new GraphDataRecorder(directory, isRunPhase, properties);
    }

    if (isRunPhase && loadGraphFile.exists()) {
      graphGenerator.prepareMaps(loadGraphFile);
    }

    return graphGenerator;
  }

  static File getLoadFile(String directory) {
    return new File(directory, LOAD_GRAPH_FILE_NAME);
  }

  static File getRunFile(String directory) {
    return new File(directory, RUN_GRAPH_FILE_NAME);
  }

  @Override
  public final Graph nextValue() {
    try {
      lastValue = createNextValue();
    } catch (IOException e) {
      e.printStackTrace();
    }

    storeGraphComponents(lastValue);

    return lastValue;
  }

  @Override
  public final Graph lastValue() {
    return lastValue;
  }

  int getProductsPerOrder() {
    return productsPerOrder;
  }

  int getComponentsPerProduct() {
    return componentsPerProduct;
  }

  int getTestParameterCount() {
    return testParameterCount;
  }

  boolean isOnlyCreateNodes() {
    return onlyCreateNodes;
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

  File getGraphFile() {
    return graphFile;
  }

  Graph getLastValue() {
    return lastValue;
  }

  private void storeGraphComponents(Graph graph) {
    for (Edge edge : graph.getEdges()) {
      if (!edgeMap.containsKey(edge.getId())) {
        Edge copyEdge = edge.copyEdge();
        edgeMap.put(copyEdge.getId(), copyEdge);
        lastEdgeId = lastEdgeId < copyEdge.getId() ? copyEdge.getId() : lastEdgeId;
      }
    }

    // The value of the node is not copied to the copyNode.
    // Since the value is not needed on later retrieval we can save a lot of memory here.
    for (Node node : graph.getNodes()) {
      if (!nodeMap.containsKey(node.getId())) {
        Node copyNode = node.copyNode();
        nodeMap.put(copyNode.getId(), copyNode);
        lastNodeId = lastNodeId < copyNode.getId() ? copyNode.getId() : lastNodeId;
      }
    }
  }

  private void prepareMaps(File loadNodeFile) throws IOException {
    for (int i = 0; i < getNumberOfGraphs(loadNodeFile); i++) {
      storeGraphComponents(getNextLoadGraph());
    }
  }

  private int getNumberOfGraphs(File file) throws IOException {
    LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));

    long num;

    do {
      num = lineNumberReader.skip(Long.MAX_VALUE);
    } while (num > 0);

    return lineNumberReader.getLineNumber();
  }

  /**
   * Load the values of the {@link Node}s and {@link Edge}s from the load phase into their containers.
   */
  abstract Graph getNextLoadGraph();

  /**
   * @return the next generated value.
   * @throws IOException if an I/O exception occurs.
   */
  abstract Graph createNextValue() throws IOException;
}