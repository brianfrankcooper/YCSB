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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestGraphDataRecorder {

  private static Properties properties = new Properties();
  private static String directoryName = System.getProperty("user.dir") + File.separator + "test";
  private static File directory = new File(directoryName);
  private final int graphsToCreate = 100;
  private int testParameterCountValue;

  public TestGraphDataRecorder(int value) {
    this.testParameterCountValue = value;
  }

  @Parameters
  public static Collection<Object> data() {
    Object[] data = {0, 1, 128};
    return Arrays.asList(data);
  }

  @BeforeClass
  public static void initPropertiesAndClearDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Before
  public void setUp() {
    directory.mkdirs();
    properties.setProperty("testparametercount", String.valueOf(this.testParameterCountValue));
  }

  @After
  public void tearDown() throws IOException, NoSuchFieldException, IllegalAccessException {
    Field field = Node.class.getDeclaredField("nodeIdCount");
    field.setAccessible(true);
    field.set(null, 0L);
    field.setAccessible(false);

    field = Edge.class.getDeclaredField("edgeIdCount");
    field.setAccessible(true);
    field.set(null, 0L);
    field.setAccessible(false);

    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testCreatingFilesInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
  }

  @Test
  public void creatingFilesTestInRunPhaseWithoutLoadFiles() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(2, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
    assertEquals(0, Node.getNodeCount());
    assertEquals(0, Edge.getEdgeCount());
  }

  @Test
  public void creatingFilesTestInRunPhaseWithLoadFiles() throws IOException {
    File nodeFile = createComponentLoadFileInDirectory(Node.NODE_IDENTIFIER);
    File edgeFile = createComponentLoadFileInDirectory(Edge.EDGE_IDENTIFIER);

    nodeFile.createNewFile();
    edgeFile.createNewFile();

    int nodeId = 10;

    setIdsToNodeLoadFile(nodeFile, nodeId);

    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Set<File> list = new HashSet<>(Arrays.asList(Objects.requireNonNull(directory.listFiles())));

    assertEquals(4, list.size());
    assertTrue(list.contains(graphDataRecorder.getNodeFile()));
    assertTrue(list.contains(graphDataRecorder.getEdgeFile()));
    assertEquals(++nodeId, Node.getNodeCount());
  }

  @Test
  public void checkIfGraphComponentsAreStoredInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecorder.createNextValue();
    }

    List<String> strings = Files.readAllLines(graphDataRecorder.getNodeFile().toPath(),
        Charset.forName(new FileReader(graphDataRecorder.getNodeFile()).getEncoding()));

    assertEquals(graphsToCreate, strings.size());
  }

  @Test
  public void checkIfGraphComponentsAreStoredInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    for (int i = 0; i < graphsToCreate; i++) {
      graphDataRecorder.createNextValue();
    }

    List<String> strings = Files.readAllLines(graphDataRecorder.getNodeFile().toPath(),
        Charset.forName(new FileReader(graphDataRecorder.getNodeFile()).getEncoding()));

    assertEquals(graphsToCreate, strings.size());
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGetInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.nextValue());
    }

    checkCreatedGraphsWithGetter(graphDataRecorder, graphList);
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGetInRunPhaseWithoutLoadFiles() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.nextValue());
    }

    checkCreatedGraphsWithGetter(graphDataRecorder, graphList);

    assertEquals(0, graphList.get(0).getNodes().get(0).getId());
    assertEquals(0, graphList.get(1).getEdges().get(0).getId());
  }

  @Test
  public void checkIfGraphComponentsCanBeRetrievedByGetInRunPhaseWithLoadFiles() throws IOException {
    int nodeId = 5;

    File nodeFile = createComponentLoadFileInDirectory(Node.NODE_IDENTIFIER);
    File edgeFile = createComponentLoadFileInDirectory(Edge.EDGE_IDENTIFIER);

    nodeFile.createNewFile();
    edgeFile.createNewFile();

    setIdsToNodeLoadFile(nodeFile, nodeId);

    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    List<Graph> graphList = new ArrayList<>();

    for (int i = 0; i < graphsToCreate; i++) {
      graphList.add(graphDataRecorder.nextValue());
    }

    checkCreatedGraphsWithGetter(graphDataRecorder, graphList);

    assertEquals(++nodeId, graphList.get(0).getNodes().get(0).getId());
  }

  @Test
  public void testLastValueInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    Graph graph = graphDataRecorder.createNextValue();

    assertEquals(graph, graphDataRecorder.lastValue());
  }

  @Test
  public void testLastValueInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    Graph graph = graphDataRecorder.createNextValue();

    assertEquals(graph, graphDataRecorder.lastValue());
  }

  @Test
  public void testLastValueWithoutCallToNextValueInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    assertEquals(0, graphDataRecorder.lastValue().getNodes().size());
    assertEquals(0, graphDataRecorder.lastValue().getEdges().size());
  }

  @Test
  public void testLastValueWithoutCallToNextValueInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    assertEquals(0, graphDataRecorder.lastValue().getNodes().size());
    assertEquals(0, graphDataRecorder.lastValue().getEdges().size());
  }

  @Test(expected = IOException.class)
  public void testCloseInLoadPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInLoadPhase();

    graphDataRecorder.createNextValue();
    graphDataRecorder.close();
    graphDataRecorder.createNextValue();
  }

  @Test(expected = IOException.class)
  public void testCloseInRunPhase() throws IOException {
    GraphDataRecorder graphDataRecorder = getGraphDataRecorderInRunPhase();

    graphDataRecorder.createNextValue();
    graphDataRecorder.close();
    graphDataRecorder.createNextValue();
  }

  private GraphDataRecorder getGraphDataRecorderInLoadPhase() throws IOException {
    return (GraphDataRecorder) GraphDataGenerator.create(directoryName, false, properties);
  }

  private GraphDataRecorder getGraphDataRecorderInRunPhase() throws IOException {
    return (GraphDataRecorder) GraphDataGenerator.create(directoryName, true, properties);
  }

  private File createComponentLoadFileInDirectory(String componentIdentifier) {
    return new File(directory, componentIdentifier + "load.json");
  }

  private File createComponentRunFileInDirectory(String componentIdentifier) {
    return new File(directory, componentIdentifier + "run.json");
  }

  private void setIdsToNodeLoadFile(File nodeFile, int nodeId) throws IOException {
    FileWriter fileWriter = new FileWriter(nodeFile);
    fileWriter.write("{\"id\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"" + nodeId + "\",\"off\":0}},\"" +
        "label\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"Factory\",\"off\":0}},\"" +
        "value\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"\\u0026Ys+Ck0\\u0027j3.\\u0026)\\u0026.1Je;)7\",\"off\":0}}}");
    fileWriter.close();
  }

  private void checkCreatedGraphsWithGetter(GraphDataRecorder graphDataRecorder, List<Graph> graphList) {
    for (Graph graph : graphList) {
      for (Node node : graph.getNodes()) {
        assertEquals(node, graphDataRecorder.getNode(node.getId()));
      }
      for (Edge edge : graph.getEdges()) {
        assertEquals(edge, graphDataRecorder.getEdge(edge.getId()));
      }
    }
  }
}