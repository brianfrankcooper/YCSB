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

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
 * A ORDER includes a PRODUCT (or multiple). - Set by the {@value PRODUCTS_PER_ORDER_PROPERTY} parameter.
 * <p>
 * A PRODUCT is produced on a DATE and have TESTS run on them.
 * <p>
 * TESTS have a TESTPARAMETER (or multiple). - Set by the {@value TEST_PARAMETER_COUNT_PROPERTY} parameter.
 */
public class GraphDataRecorder extends GraphDataGenerator implements Closeable {

  private Node factory;
  private Node orders;
  private Node machine;
  private Node design;
  private Node currentOrder;
  private Node product;
  private Node component;
  private Node tests;

  private boolean shouldCreateProduct = true;
  private boolean shouldCreateDate = true;
  private boolean shouldCreateTests = true;
  private boolean shouldCreateComponent = true;
  private int productsInOrderCounter = 0;
  private int productCounter = 0;
  private int componentCounter = 0;
  private int testCounter = 0;

  private Map<String, FileWriter> fileWriterMap;

  GraphDataRecorder(String outputDirectory, boolean isRunPhase, Properties properties) throws IOException {
    super(outputDirectory, isRunPhase, properties);

    fileWriterMap = new HashMap<>();
  }

  @Override
  Graph getNextLoadGraph() {
    return createGraph();
  }

  @Override
  Graph createNextValue() throws IOException {
    Graph graph = createGraph();

    saveGraphContentsAndFillValueOfNodes(graph);

    return graph;
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
    graph.getNodes().forEach(Node::getHashMap);
    insert(getGraphFile(), graph);
  }

  private Graph createGraph() {
    Graph graph = new Graph();

    if (factory == null) {
      this.factory = new Node("Factory");
      graph.addNode(this.factory);
    } else if (machine == null) {
      this.machine = new Node("Machine");
      graph.addNode(this.machine);
      graph.addEdge(new Edge("owns", factory.getId(), machine.getId()));
    } else if (orders == null) {
      this.orders = new Node("Orders");
      graph.addNode(this.orders);
      graph.addEdge(new Edge("has", factory.getId(), orders.getId()));
    } else if (design == null) {
      this.design = new Node("Design");
      graph.addNode(this.design);
      graph.addEdge(new Edge("builds", machine.getId(), design.getId()));
    } else if (productsInOrderCounter == 0) {
      currentOrder = new Node("Order");
      graph.addNode(currentOrder);
      graph.addEdge(new Edge("have", orders.getId(), currentOrder.getId()));
      productsInOrderCounter = getProductsPerOrder();
    } else if (shouldCreateProduct) {
      product = new Node("Product");
      graph.addNode(product);
      graph.addEdge(new Edge("ordered", currentOrder.getId(), product.getId()));
      graph.addEdge(new Edge("templateFor", design.getId(), product.getId()));
      graph.addEdge(new Edge("produced", machine.getId(), product.getId()));
      productCounter++;
      shouldCreateProduct = false;
    } else if (shouldCreateDate) {
      Node date = new Node("Date");
      graph.addNode(date);
      graph.addEdge(new Edge("producedOn", product.getId(), date.getId()));
      shouldCreateDate = false;
    } else if (shouldCreateComponent) {
      component = new Node("Component");
      graph.addNode(component);
      graph.addEdge(new Edge("madeOf", product.getId(), component.getId()));
      componentCounter++;
      shouldCreateComponent = false;
    } else if (shouldCreateTests) {
      tests = new Node("Tests");
      graph.addNode(tests);
      graph.addEdge(new Edge("undergoes", component.getId(), tests.getId()));
      shouldCreateTests = false;
    } else if (testCounter < getTestParameterCount()) {
      Node testParameterNode = new Node("TestParameterNr:" + testCounter);
      graph.addNode(testParameterNode);
      graph.addEdge(new Edge("include", tests.getId(), testParameterNode.getId()));
      testCounter++;
    }

    if (areAllProductsReadyForOrder()) {
      resetEverythingForNewOrder();
    } else if (areAllComponentsAddedToProduct()) {
      resetEverythingForNewProduct();
    } else if (isTestingFinished()) {
      resetEverythingForNewComponent();
    }

    if (isOnlyCreateNodes()) {
      graph.getEdges().clear();
    }

    return graph;
  }

  private boolean areAllProductsReadyForOrder() {
    return productCounter == getProductsPerOrder() && areAllComponentsAddedToProduct() && isTestingFinished();
  }

  private boolean areAllComponentsAddedToProduct() {
    return componentCounter == getComponentsPerProduct() && isTestingFinished();
  }

  private boolean isTestingFinished() {
    return testCounter == getTestParameterCount();
  }

  private void resetEverythingForNewOrder() {
    productsInOrderCounter = 0;
    productCounter = 0;
    resetEverythingForNewProduct();
  }

  private void resetEverythingForNewProduct() {
    shouldCreateProduct = true;
    shouldCreateDate = true;
    componentCounter = 0;
    resetEverythingForNewComponent();
  }

  private void resetEverythingForNewComponent() {
    shouldCreateComponent = true;
    shouldCreateTests = true;
    testCounter = 0;
  }

  private void insert(File file, Graph graph) throws IOException {
    String output = getGson().toJson(graph, getValueType());
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
