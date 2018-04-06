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
import com.yahoo.ycsb.generator.graph.Graph;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import com.yahoo.ycsb.generator.graph.randomcomponents.RandomGraphComponentGenerator;
import com.yahoo.ycsb.generator.operationorder.OperationOrderGenerator;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

    return insertGraphComponents(db, graph.getNodes()) && insertGraphComponents(db, graph.getEdges());
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    String nextOperation = orderGenerator.nextValue();

    return executeOperation(nextOperation, db, graphDataGenerator);
  }

  private boolean executeOperation(String operation, DB db, Generator<Graph> generator) {
    if (operation == null) {
      return false;
    }

    switch (operation) {
    case READ_IDENTIFIER:
      doTransactionRead(db);
      break;
    case UPDATE_IDENTIFIER:
      doTransactionUpdate(db);
      break;
    case INSERT_IDENTIFIER:
      doTransactionInsert(db, generator);
      break;
    case SCAN_IDENTIFIER:
      doTransactionScan(db);
      break;
    case READMODIFYWRITE_IDENTIFIER:
      System.err.println("Read Modify Write");
      doTransactionReadModifyWrite(db);
    default:
      System.err.println("Unsupported operation was chosen.");
      return false;
    }

    return true;
  }

  private void doTransactionInsert(DB db, Generator<Graph> generator) {
    Graph graph = generator.nextValue();

    insertGraphComponents(db, graph.getNodes());
    insertGraphComponents(db, graph.getEdges());
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
  }

  private void doTransactionUpdate(DB db) {
    GraphComponent graphComponent = randomGraphComponentGenerator.nextValue();

    if (graphComponent != null) {
      db.update(graphComponent.getComponentTypeIdentifier(),
          String.valueOf(graphComponent.getId()),
          graphComponent.getHashMap());
    }
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
  }

  private void doTransactionReadModifyWrite(DB db) {
    Node node = randomGraphComponentGenerator.chooseRandomNode();
    Node otherNode = randomGraphComponentGenerator.chooseRandomNode();
    Map<String, ByteIterator> values = new HashMap<>();

    db.read(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), Node.NODE_FIELDS_SET, values);

    values = node.getHashMap();
    values.put(Node.VALUE_IDENTIFIER, otherNode.getHashMap().get(Node.VALUE_IDENTIFIER));

    db.update(node.getComponentTypeIdentifier(), String.valueOf(node.getId()), values);
  }

  private boolean insertGraphComponents(DB db, List<? extends GraphComponent> graphComponents) {
    for (GraphComponent graphComponent : graphComponents) {
      Map<String, ByteIterator> values = graphComponent.getHashMap();

      if (!db.insert(graphComponent.getComponentTypeIdentifier(), String.valueOf(graphComponent.getId()), values)
          .isOk()) {
        return false;
      }
    }
    return true;
  }
}
