/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.graph.*;
import com.yahoo.ycsb.generator.graph.randomcomponents.RandomGraphComponentGenerator;
import com.yahoo.ycsb.generator.operationorder.OperationOrderGenerator;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static com.yahoo.ycsb.workloads.CoreWorkload.*;
import static java.io.File.separatorChar;

/**
 * Workload class for graph data.
 * <p>
 * This workload will store the generated values in ycsbDir/benchmarkingData to be able to reproduce the
 * benchmark run with the same data on multiple databases.
 * You can change the directory with the parameter {@value DATA_SET_DIRECTORY_PROPERTY}.
 * <p>
 * Every node will have a size of 500 Bytes by default.
 * This can be changed via the {@value NODE_BYTE_SIZE_PROPERTY} parameter.
 * <p>
 * The recordCount property determines how many nodes will be inserted. The total amount of database inserts could
 * be higher due to edges being inserted.
 */
public class GraphWorkload extends Workload {

  public static final String DATA_SET_DIRECTORY_PROPERTY = "datasetdirectory";
  private static final String DATA_SET_DIRECTORY_DEFAULT = new File(System.getProperty("user.dir"),
      "benchmarkingData").getAbsolutePath();

  private static final String NODE_BYTE_SIZE_PROPERTY = "nodebytesize";
  private static final String NODE_BYTE_SIZE_DEFAULT = "500";
  private static int nodeByteSize = Integer.parseInt(NODE_BYTE_SIZE_DEFAULT);

  private int maxScanLength;
  private GraphDataGenerator graphDataGenerator;
  private OperationOrderGenerator orderGenerator;
  private RandomGraphComponentGenerator randomGraphComponentGenerator;

  /**
   * @return the value set via the {@value NODE_BYTE_SIZE_PROPERTY} property. The default value is {@value
   * NODE_BYTE_SIZE_DEFAULT}.
   */
  public static int getNodeByteSize() {
    return nodeByteSize;
  }

  private static String getOutputDirectory(Properties properties) throws WorkloadException {
    String outputDirectory = properties.getProperty(DATA_SET_DIRECTORY_PROPERTY, DATA_SET_DIRECTORY_DEFAULT);

    if (outputDirectory.charAt(outputDirectory.length() - 1) != separatorChar) {
      outputDirectory += separatorChar;
    }

    File directory = new File(outputDirectory);

    if (!directory.exists() && !directory.mkdirs()) {
      throw new WorkloadException(outputDirectory + " does not exist and cannot be created.");
    }

    return outputDirectory;
  }

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);

    nodeByteSize = Integer.parseInt(properties.getProperty(NODE_BYTE_SIZE_PROPERTY, NODE_BYTE_SIZE_DEFAULT));
    maxScanLength = Integer.parseInt(properties.getProperty(MAX_SCAN_LENGTH_PROPERTY,
        MAX_SCAN_LENGTH_PROPERTY_DEFAULT));

    String outputDirectory = getOutputDirectory(properties);
    boolean isRunPhase = Boolean.parseBoolean(properties.getProperty(Client.DO_TRANSACTIONS_PROPERTY));

    try {
      graphDataGenerator = GraphDataGenerator.create(outputDirectory, isRunPhase, properties);
      randomGraphComponentGenerator = RandomGraphComponentGenerator.create(outputDirectory, isRunPhase,
          graphDataGenerator);
      orderGenerator = OperationOrderGenerator.create(outputDirectory,
          isRunPhase,
          createOperationGenerator(properties));
    } catch (IOException e) {
      throw new WorkloadException(e);
    }
  }

  @Override
  public boolean doInsert(DB db, Object threadState) {
    Graph graph = graphDataGenerator.nextValue();

    return insertGraphComponents(db, graph);
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    String nextOperation = orderGenerator.nextValue();

    return executeOperation(nextOperation, db, graphDataGenerator);
  }

  private boolean insertGraphComponents(DB db, Graph graph) {
    System.out.println("Inserting Nodes");
    for (Node node : graph.getNodes()) {
      if (!insertNode(db, node)) {
        return false;
      }
    }

    System.out.println("Inserting Edges");
    for (Edge edge : graph.getEdges()) {
      if (!insertEdge(db, edge)) {
        return false;
      }
    }
    return true;
  }

  private boolean executeOperation(String operation, DB db, Generator<Graph> generator) {
    if (operation == null) {
      return false;
    }

    switch (operation) {
    case READ_IDENTIFIER:
      System.out.println("Reading");
      doTransactionRead(db);
      break;
    case UPDATE_IDENTIFIER:
      System.out.println("Updating");
      doTransactionUpdate(db);
      break;
    case INSERT_IDENTIFIER:
      System.out.println("Inserting");
      doTransactionInsert(db, generator);
      break;
    case SCAN_IDENTIFIER:
      System.out.println("Scanning");
      doTransactionScan(db);
      break;
    case READMODIFYWRITE_IDENTIFIER:
      System.out.println("ReadingModifyingWriting");
      doTransactionReadModifyWrite(db);
    default:
      System.err.println("Unsupported operation was chosen.");
      return false;
    }

    return true;
  }

  private void doTransactionInsert(DB db, Generator<Graph> generator) {
    Graph graph = generator.nextValue();

    System.out.println("Inserting Nodes");
    for (Node node : graph.getNodes()) {
      insertNode(db, node);
    }

    System.out.println("Inserting Edges");
    for (Edge edge : graph.getEdges()) {
      insertEdge(db, edge);
    }
  }

  private void doTransactionRead(DB db) {
    GraphComponent graphComponent = randomGraphComponentGenerator.nextValue();

    Map<String, ByteIterator> map = new HashMap<>();

    if (graphComponent != null) {
      db.read(graphComponent.getComponentTypeIdentifier(),
          String.valueOf(graphComponent.getId()),
          graphComponent.getFieldSet(),
          map
      );
    }

    printMap(map);
  }

  private void doTransactionUpdate(DB db) {
    GraphComponent graphComponent = randomGraphComponentGenerator.nextValue();

    Map<String, ByteIterator> map = new HashMap<>();

    if (graphComponent != null) {
      db.read(graphComponent.getComponentTypeIdentifier(),
          String.valueOf(graphComponent.getId()),
          graphComponent.getFieldSet(),
          map);

      db.update(graphComponent.getComponentTypeIdentifier(),
          String.valueOf(graphComponent.getId()),
          map);
    }

    printMap(map);
  }

  private void doTransactionScan(DB db) {
    GraphComponent graphComponent = randomGraphComponentGenerator.nextValue();

    Vector<HashMap<String, ByteIterator>> hashMaps = new Vector<>();

    if (graphComponent != null) {
      db.scan(graphComponent.getComponentTypeIdentifier(),
          String.valueOf(graphComponent.getId()),
          maxScanLength,
          graphComponent.getFieldSet(),
          hashMaps
      );
    }

    for (HashMap<String, ByteIterator> hashMap : hashMaps) {
      printMap(hashMap);
    }
  }

  private void doTransactionReadModifyWrite(DB db) {
    Node node = randomGraphComponentGenerator.chooseRandomNode();
    Map<String, ByteIterator> values = new HashMap<>();

    db.read(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), Node.NODE_FIELDS_SET, values);

    System.out.println("old");
    printMap(values);

    values = node.getHashMap();

    System.out.println("new");
    printMap(values);

    db.update(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), values);
  }

  private void printMap(Map<String, ByteIterator> map) {
    for (String s : map.keySet()) {
      System.out.println("Key: " + s + " Value: " + map.get(s));
    }
  }

  private boolean insertNode(DB db, Node node) {
    Map<String, ByteIterator> values = node.getHashMap();

    System.out.println("Node: " + node.getId());
    printMap(values);

    return db.insert(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), values).isOk();
  }

  private boolean insertEdge(DB db, Edge edge) {
    Map<String, ByteIterator> values = edge.getHashMap();

    System.out.println("Edge: " + edge.getId());
    printMap(values);

    return db.insert(edge.getComponentTypeIdentifier(), String.valueOf(edge.getId()), values).isOk();
  }
}
