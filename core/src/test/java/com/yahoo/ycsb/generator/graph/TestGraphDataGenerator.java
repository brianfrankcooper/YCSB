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

package com.yahoo.ycsb.generator.graph;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGraphDataGenerator {

  private static File testDirectory;
  private static Properties properties;
  private static int numberOfNodes;

  @BeforeClass
  public static void setUp() {
    testDirectory = new File(System.getProperty("user.dir"), "test");

    properties = new Properties();
    properties.setProperty("testparametercount", "1");

    numberOfNodes = 1000;
  }

  @AfterClass
  public static void deleteDirectory() throws IOException {
    FileUtils.deleteDirectory(testDirectory);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(testDirectory);
  }

  @Test
  public void testLoadPhase() throws IOException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataRecreator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  @Test
  public void testRunPhase() throws IOException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataRecreator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  @Test
  public void testRunPhaseWithLoadData() throws IOException, NoSuchFieldException, IllegalAccessException {
    GraphDataGenerator recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), false, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    for (int i = 0; i < numberOfNodes; i++) {
      recorder.nextValue();
    }

    Field field = Node.class.getDeclaredField("nodeIdCount");
    field.setAccessible(true);
    field.set(null, 0L);
    field.setAccessible(false);

    field = Edge.class.getDeclaredField("edgeIdCount");
    field.setAccessible(true);
    field.set(null, 0L);
    field.setAccessible(false);

    recorder = GraphDataGenerator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recorder instanceof GraphDataRecorder);

    ArrayList<Graph> graphs = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphs.add(recorder.nextValue());
    }

    GraphDataGenerator recreator = GraphDataGenerator.create(testDirectory.getAbsolutePath(), true, properties);

    assertTrue(recreator instanceof GraphDataRecreator);

    compareRecreatedGraphs(graphs, recreator);
  }

  private void compareRecreatedGraphs(ArrayList<Graph> graphs, GraphDataGenerator recreator) {
    for (Graph originalGraph : graphs) {
      Graph recreatedGraph = recreator.nextValue();

      assertEquals(originalGraph.getNodes().size(), recreatedGraph.getNodes().size());
      assertEquals(originalGraph.getEdges().size(), recreatedGraph.getEdges().size());

      List<Node> originalNodes = originalGraph.getNodes();
      List<Node> recreatedNodes = recreatedGraph.getNodes();
      for (int i = 0; i < originalNodes.size(); i++) {
        Node originalNode = originalNodes.get(i);
        Node recreatedNode = recreatedNodes.get(i);

        assertEquals(originalNode.getId(), recreatedNode.getId());
        assertEquals(originalNode.getLabel(), recreatedNode.getLabel());
        assertEquals(originalNode.getHashMap().toString(), recreatedNode.getHashMap().toString());
      }

      List<Edge> originalEdges = originalGraph.getEdges();
      List<Edge> recreatedEdges = recreatedGraph.getEdges();
      for (int i = 0; i < originalEdges.size(); i++) {
        Edge originalEdge = originalEdges.get(i);
        Edge recreatedEdge = recreatedEdges.get(i);

        assertEquals(originalEdge.getId(), recreatedEdge.getId());
        assertEquals(originalEdge.getLabel(), recreatedEdge.getLabel());
        assertEquals(originalEdge.getStartNode().getId(), recreatedEdge.getStartNode().getId());
        assertEquals(originalEdge.getEndNode().getId(), recreatedEdge.getEndNode().getId());
      }
    }
  }
}
