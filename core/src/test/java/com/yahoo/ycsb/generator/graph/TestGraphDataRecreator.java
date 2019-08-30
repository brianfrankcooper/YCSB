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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestGraphDataRecreator {

  private static int numberOfNodesDuringLoad;
  private static int numberOfNodes;
  private static String directory = "src" + File.separator + "test" + File.separator + "resources";

  @BeforeClass
  public static void setNumberOfNodes() throws IOException {
    File graphLoadFile = new File(directory, Graph.GRAPH_IDENTIFIER+ "load.json");
    File graphRunFile = new File(directory, Graph.GRAPH_IDENTIFIER + "run.json");

    numberOfNodesDuringLoad = Files.readAllLines(graphLoadFile.toPath(), Charset.forName(new FileReader(graphLoadFile)
        .getEncoding())).size();
    numberOfNodes = Files.readAllLines(graphRunFile.toPath(), Charset.forName(new FileReader(graphRunFile)
        .getEncoding())).size();
  }

  @Test
  public void loadFilesInLoadPhaseTest() throws IOException {
    getGraphDataRecreatorInLoadPhase();
  }

  @Test
  public void loadFilesInRunPhaseTest() throws IOException {
    getGraphDataRecreatorInRunPhase();
  }

  @Test(expected = IOException.class)
  public void tryLoadingLoadFilesButFailBecauseFolderDoesNotContainNecessaryFiles() throws IOException {
    new GraphDataRecreator(System.getProperty("user.dir"), false, new Properties());
  }

  @Test(expected = IOException.class)
  public void tryLoadingRunFilesButFailBecauseFolderDoesNotContainNecessaryFiles() throws IOException {
    new GraphDataRecreator(System.getProperty("user.dir"), true, new Properties());
  }

  @Test
  public void checkIfRecreationProducesCorrectGraphsFromLoadPhase() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInLoadPhase();

    List<Graph> graphList = new ArrayList<>();

    checkCorrectGraphStructure(graphDataRecreator, graphList, numberOfNodesDuringLoad);

    assertEquals(numberOfNodesDuringLoad, graphList.size());

    // all contents in file read.
    assertEquals(0, graphDataRecreator.createNextValue().getNodes().size());
    assertEquals(0, graphDataRecreator.createNextValue().getEdges().size());
  }

  @Test
  public void checkIfRecreationProducesCorrectGraphsFromRunPhase() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInRunPhase();

    List<Graph> graphList = new ArrayList<>();

    checkCorrectGraphStructure(graphDataRecreator, graphList, numberOfNodes);

    assertEquals(numberOfNodes, graphList.size());

    // all contents in file read.
    assertEquals(0, graphDataRecreator.createNextValue().getNodes().size());
    assertEquals(0, graphDataRecreator.createNextValue().getEdges().size());
  }

  @Test
  public void checkIfRecreatedGraphsAreRetrievableByGetInLoadPhase() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInLoadPhase();

    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < numberOfNodesDuringLoad; i++) {
      graphList.add(graphDataRecreator.nextValue());
    }

    compareGraphComponents(graphDataRecreator, graphList);
  }

  @Test
  public void checkIfRecreatedGraphsAreRetrievableByGetInRunPhase() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInRunPhase();

    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < numberOfNodes; i++) {
      graphList.add(graphDataRecreator.nextValue());
    }

    compareGraphComponents(graphDataRecreator, graphList);
  }

  @Test
  public void lastValueInLoadPhaseTest() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInLoadPhase();

    Graph graph;

    for (int i = 0; i < numberOfNodesDuringLoad; i++) {
      graph = graphDataRecreator.nextValue();
      Graph actual = graphDataRecreator.lastValue();
      assertEquals(graph, actual);
    }

    graphDataRecreator.nextValue();
    graph = graphDataRecreator.lastValue();
    assertEquals(0, graph.getNodes().size());
    assertEquals(0, graph.getEdges().size());
  }

  @Test
  public void lastValueInRunPhaseTest() throws IOException {
    GraphDataRecreator graphDataRecreator = getGraphDataRecreatorInRunPhase();

    Graph graph;

    for (int i = 0; i < numberOfNodes; i++) {
      graph = graphDataRecreator.nextValue();
      Graph actual = graphDataRecreator.lastValue();
      assertEquals(graph, actual);
    }

    graphDataRecreator.nextValue();
    graph = graphDataRecreator.lastValue();
    assertEquals(0, graph.getNodes().size());
    assertEquals(0, graph.getEdges().size());
  }


  private GraphDataRecreator getGraphDataRecreatorInLoadPhase() throws IOException {
    return new GraphDataRecreator(directory, false, new Properties());
  }

  private GraphDataRecreator getGraphDataRecreatorInRunPhase() throws IOException {
    return new GraphDataRecreator(directory, true, new Properties());
  }

  private void checkCorrectGraphStructure(GraphDataRecreator graphDataRecreator, List<Graph> graphList, int numberOfNodes) throws IOException {
    for (int i = 0; i < numberOfNodes; i++) {
      Graph graph = graphDataRecreator.nextValue();
      graphList.add(graph);

      assertEquals(1, graph.getNodes().size());

      switch (graph.getNodes().get(0).getLabel()) {
        case "Product":
          assertEquals(3, graph.getEdges().size());
          break;
        case "Factory":
          assertEquals(0, graph.getEdges().size());
          break;
        default:
          assertEquals(1, graph.getEdges().size());
          break;
      }
    }
  }

  private void compareGraphComponents(GraphDataRecreator graphDataRecreator, List<Graph> graphList) {
    for (Graph graph : graphList) {
      for (Node node : graph.getNodes()) {
        assertEquals(node.getLabel(), graphDataRecreator.getNode(node.getId()).getLabel());
        assertEquals(node.getId(), graphDataRecreator.getNode(node.getId()).getId());
      }

      for (Edge edge : graph.getEdges()) {
        assertEquals(edge.getHashMap().toString(), graphDataRecreator.getEdge(edge.getId()).getHashMap().toString());
      }
    }
  }
}