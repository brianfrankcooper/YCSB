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

package com.yahoo.ycsb.generator.graph.randomcomponents;

import com.yahoo.ycsb.generator.StoringGenerator;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;

import java.io.File;
import java.io.IOException;

/**
 * Abstract class to pick a random {@link GraphComponent} ({@link Node} or {@link Edge}).
 */
public abstract class RandomGraphComponentGenerator extends StoringGenerator<GraphComponent> {

  private static final String NODE_FILE_NAME = "nodeIds.txt";
  private static final String EDGE_FILE_NAME = "edgeIds.txt";
  private static final String COMPONENT_FILE_NAME = "componentIds.txt";
  private static final String CLASS_NAME = RandomGraphComponentGenerator.class.getSimpleName();

  private final File nodeFile;
  private final File edgeFile;
  private final File componentFile;
  private final GraphDataGenerator graphDataGenerator;
  private GraphComponent lastGraphComponent;

  RandomGraphComponentGenerator(String directory, GraphDataGenerator graphDataGenerator) throws IOException {
    this.graphDataGenerator = graphDataGenerator;

    File directoryFile = new File(directory);

    nodeFile = new File(directory, NODE_FILE_NAME);
    edgeFile = new File(directory, EDGE_FILE_NAME);

    componentFile = new File(directory, COMPONENT_FILE_NAME);

    if (!checkFiles(directoryFile, nodeFile, edgeFile, componentFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  /**
   * Creates a {@link RandomGraphComponentRecorder} or a {@link RandomGraphComponentRecreator} depending on the given
   * values.
   *
   * @param directory          which contains the recorded data or where the data will be recorded to.
   * @param isRunPhase         tells the current execution phase (load or run).
   * @param graphDataGenerator to get the actual {@link GraphComponent}s. This {@link GraphDataGenerator} has to be
   *                           used during the benchmark.
   * @return a subclass of the {@link RandomGraphComponentGenerator} or null if it's not the run phase (during load
   * this is not needed).
   * @throws IOException if an I/O exception occurs.
   */
  public static RandomGraphComponentGenerator create(String directory,
                                                     boolean isRunPhase,
                                                     GraphDataGenerator graphDataGenerator) throws IOException {
    if (isRunPhase) {
      if (checkDataPresentAndCleanIfSomeMissing(CLASS_NAME,
          new File(directory, NODE_FILE_NAME),
          new File(directory, EDGE_FILE_NAME),
          new File(directory, COMPONENT_FILE_NAME)
      )) {
        System.out.println(CLASS_NAME + " creating RECREATOR.");
        return new RandomGraphComponentRecreator(directory, graphDataGenerator);
      } else {
        System.out.println(CLASS_NAME + " creating RECORDER.");
        return new RandomGraphComponentRecorder(directory, graphDataGenerator);
      }
    } else {
      System.out.println(CLASS_NAME + " not needed during load phase. Nothing created.");
      return null;
    }
  }

  @Override
  public final GraphComponent nextValue() {
    switch (randomNodeOrEdge()) {
    case NODE:
      lastGraphComponent = chooseRandomNode();
      break;
    case EDGE:
      lastGraphComponent = chooseRandomEdge();
      break;
    default:
      return null;
    }

    return lastGraphComponent;
  }

  @Override
  public final GraphComponent lastValue() {
    return lastGraphComponent;
  }

  /**
   * @return a randomly chosen {@link Node}.
   */
  public final Node chooseRandomNode() {
    long id = chooseRandomNodeId();

    return graphDataGenerator.getNode(id);
  }

  GraphDataGenerator getGraphDataGenerator() {
    return graphDataGenerator;
  }

  File getNodeFile() {
    return nodeFile;
  }

  File getEdgeFile() {
    return edgeFile;
  }

  File getComponentFile() {
    return componentFile;
  }

  private Edge chooseRandomEdge() {
    long id = chooseRandomEdgeId();

    return graphDataGenerator.getEdge(id);
  }

  /**
   * @return a random {@link Node} id which is already present.
   */
  abstract long chooseRandomNodeId();

  /**
   * @return a random {@link Edge} id which is already present.
   */
  abstract long chooseRandomEdgeId();

  /**
   * @return randomly on of the two possible values in {@link RandomComponent}.
   */
  abstract RandomComponent randomNodeOrEdge();

  enum RandomComponent {
    NODE,
    EDGE,
    INVALID;

    static RandomComponent getRandomComponent(String value) {
      if (NODE.name().equals(value)) {
        return NODE;
      } else if (EDGE.name().equals(value)) {
        return EDGE;
      } else {
        return INVALID;
      }
    }
  }
}
