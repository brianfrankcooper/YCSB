/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */

package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Generates graph data with an "industrial" structure.
 * <p>
 * Structure:
 * <p>
 * At the top is the FACTORY.
 * <p>
 * A FACTORY has MACHINEs, DESIGNs, and receives ORDERS.
 * <p>
 * A MACHINE produces a PRODUCT.
 * <p>
 * ORDERS have single ORDERs under them.
 * <p>
 * A ORDER includes a PRODUCT (or multiple). - Set by the {@value PRODUCTS_PER_ORDER_KEY} parameter.
 * <p>
 * A PRODUCT is produced on a DATE and have TESTS run on them.
 * <p>
 * TESTS have a TESTPARAMETER (or multiple). - Set by the {@value TEST_PARAMETER_COUNT_KEY} parameter.
 */
public class GraphDataRecorder extends GraphDataGenerator implements Closeable {

  private static final String TEST_PARAMETER_COUNT_KEY = "testparametercount";
  private static final int TEST_PARAMETER_COUNT_DEFAULT_VALUE = 128;
  private static final String PRODUCTS_PER_ORDER_KEY = "productsperorder";
  private static final int PRODUCTS_PER_ORDER_DEFAULT_VALUE = 10;

  private final int testParameterCount;
  private final int productsPerOrder;

  private Node factory;
  private Node orders;
  private Node machine;
  private Node design;
  private Node product;
  private Node tests;
  private Node currentOrder;

  private int productPerOrderCounter = 0;
  private boolean shouldCreateProduct = true;
  private boolean shouldCreateDate = true;
  private boolean shouldCreateTests = true;
  private int testCounter = 0;

  private Map<String, FileWriter> fileWriterMap;

  GraphDataRecorder(String outputDirectory, boolean isRunPhase, Properties properties) throws IOException {
    super(outputDirectory, isRunPhase);

    testParameterCount = Integer.valueOf(properties.getProperty(TEST_PARAMETER_COUNT_KEY,
        String.valueOf(TEST_PARAMETER_COUNT_DEFAULT_VALUE)));
    productsPerOrder = Integer.valueOf(properties.getProperty(PRODUCTS_PER_ORDER_KEY,
        String.valueOf(PRODUCTS_PER_ORDER_DEFAULT_VALUE)));

    fileWriterMap = new HashMap<>();
  }

  @Override
  List<Graph> getGraphs(int numberOfGraphs) {
    List<Graph> result = new ArrayList<>();

    for (int i = 0; i <= numberOfGraphs; i++) {
      result.add(createGraph());
    }

    return result;
  }

  @Override
  Graph createNextValue() throws IOException {
    setLastValue(createGraph());

    saveGraphContentsAndFillValueOfNodes(getLastValue());

    return getLastValue();
  }

  @Override
  public String getExceptionMessage() {
    return "Could not create graph data files or they are already present.";
  }

  @Override
  public boolean checkFiles(File directory, File... files) throws IOException {
    boolean directoryPresent = directory.exists() || directory.mkdirs();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.createNewFile();
    }

    return directoryPresent && filesCreated;
  }

  @Override
  public void close() throws IOException {
    for (String key : fileWriterMap.keySet()) {
      fileWriterMap.get(key).close();
    }
  }

  private void saveGraphContentsAndFillValueOfNodes(Graph graph) throws IOException {
    for (Node node : graph.getNodes()) {
      insert(getNodeFile(), node.getHashMap());
    }

    for (Edge edge : graph.getEdges()) {
      insert(getEdgeFile(), edge.getHashMap());
    }
  }

  private Graph createGraph() {
    Graph graph = new Graph();

    if (factory == null) {
      this.factory = new Node("Factory");
      graph.addNode(this.factory);
    } else if (machine == null) {
      this.machine = new Node("Machine");
      graph.addNode(this.machine);
      graph.addEdge(new Edge("owns", factory, machine));
    } else if (orders == null) {
      this.orders = new Node("Orders");
      graph.addNode(this.orders);
      graph.addEdge(new Edge("receives", factory, orders));
    } else if (design == null) {
      this.design = new Node("Design");
      graph.addNode(this.design);
      graph.addEdge(new Edge("builds", factory, design));
    } else if (productPerOrderCounter == 0) {
      currentOrder = new Node("Order");
      graph.addNode(currentOrder);
      graph.addEdge(new Edge("ordered", orders, currentOrder));
      productPerOrderCounter = productsPerOrder;
    } else if (shouldCreateProduct) {
      product = new Node("Product");
      graph.addNode(product);
      graph.addEdge(new Edge("produced", machine, product));
      graph.addEdge(new Edge("includes", currentOrder, product));
      shouldCreateProduct = false;
    } else if (shouldCreateDate) {
      Node date = new Node("Date");
      graph.addNode(date);
      graph.addEdge(new Edge("producedAt", product, date));
      shouldCreateDate = false;
    } else if (shouldCreateTests) {
      tests = new Node("Tests");
      graph.addNode(tests);
      graph.addEdge(new Edge("tested", product, tests));
      shouldCreateTests = false;
    } else if (testCounter < testParameterCount) {
      Node testParameterNode = new Node("TestParameterNr:" + testCounter);
      graph.addNode(testParameterNode);
      graph.addEdge(new Edge("hasTested", tests, testParameterNode));
      testCounter++;
    }

    if (isTestingFinished()) {
      shouldCreateProduct = true;
      shouldCreateDate = true;
      shouldCreateTests = true;
      testCounter = 0;
      productPerOrderCounter--;
    }

    return graph;
  }

  private boolean isTestingFinished() {
    return testCounter == testParameterCount && !shouldCreateProduct && !shouldCreateDate && !shouldCreateTests;
  }

  private void insert(File file, Map<String, ByteIterator> values) throws IOException {
    String output = getGson().toJson(values, getValueType());
    FileWriter fileWriter = getFileWriter(file);

    writeToFile(output, fileWriter);
  }

  private FileWriter getFileWriter(File file) throws IOException {
    if (!fileWriterMap.containsKey(file.getName())) {

      FileWriter fileWriter = new FileWriter(file, true);
      fileWriterMap.put(file.getName(), fileWriter);
    }

    return fileWriterMap.get(file.getName());
  }

  private void writeToFile(String output, FileWriter fileWriter) throws IOException {
    fileWriter.write(output);
    fileWriter.write("\n");
    fileWriter.flush();
  }
}
