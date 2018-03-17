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

import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.GraphComponent;
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRandomGraphComponentRecorder {

  private static String directory = System.getProperty("user.dir") + File.separator + "test";
  private static RandomGraphComponentRecorder randomGraphComponentRecorder;
  private static GraphDataGenerator generator;
  private static Edge mockedEdge;
  private static Node mockedNode;
  private int numberOfTimes = 100;

  @BeforeClass
  public static void initGenerator() throws IOException {
    generator = mock(GraphDataGenerator.class);
    mockedEdge = mock(Edge.class);
    mockedNode = mock(Node.class);
    when(generator.getEdge(anyLong())).thenReturn(mockedEdge);
    when(generator.getNode(anyLong())).thenReturn(mockedNode);

    FileUtils.deleteDirectory(directory);
  }

  @Before
  public void setUpRecorder() throws IOException {
    randomGraphComponentRecorder = new RandomGraphComponentRecorder(directory, generator);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testCreationOfFiles() {
    File file = new File(directory);
    assertEquals(3, file.listFiles().length);
  }

  @Test(expected = IOException.class)
  public void createWithFilesPresent() throws IOException {
    new File(directory, "nodeIds.txt").createNewFile();

    new RandomGraphComponentRecorder(directory, generator);
  }

  @Test
  public void chooseRandomEdgeId() throws IOException, IllegalAccessException, NoSuchFieldException {
    Field field = Edge.class.getDeclaredField("edgeIdCount");
    field.setAccessible(true);
    field.set(null, 50L);
    field.setAccessible(false);
    List<Long> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.chooseRandomEdgeId());
    }

    List<String> lines = getLines(new File(directory, "edgeIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Long created = results.get(i);
      Long stored = Long.parseLong(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test
  public void chooseRandomNodeId() throws IOException, IllegalAccessException, NoSuchFieldException {
    Field field = Node.class.getDeclaredField("nodeIdCount");
    field.setAccessible(true);
    field.set(null, 50L);
    field.setAccessible(false);

    List<Long> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.chooseRandomNodeId());
    }

    List<String> lines = getLines(new File(directory, "nodeIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      Long created = results.get(i);
      Long stored = Long.parseLong(lines.get(i));

      assertEquals(created, stored);
    }
  }

  @Test
  public void chooseRandomNodeOrEdgeId() throws IOException, IllegalAccessException, NoSuchFieldException {
    Field field = Node.class.getDeclaredField("nodeIdCount");
    field.setAccessible(true);
    field.set(null, 50L);
    field.setAccessible(false);

    field = Edge.class.getDeclaredField("edgeIdCount");
    field.setAccessible(true);
    field.set(null, 50L);
    field.setAccessible(false);

    List<RandomGraphComponentGenerator.RandomComponent> results = new ArrayList<>();

    for (int i = 0; i < numberOfTimes; i++) {
      results.add(randomGraphComponentRecorder.randomNodeOrEdge());
    }

    List<String> lines = getLines(new File(directory, "componentIds.txt"));

    assertEquals(numberOfTimes, lines.size());

    for (int i = 0; i < results.size(); i++) {
      String created = results.get(i).name();
      String stored = lines.get(i);

      assertEquals(created, stored);
    }
  }

  @Test(timeout = 100)
  public void testNextValue() {
    boolean hadNode = false;
    boolean hadEdge = false;

    when(mockedNode.getComponentTypeIdentifier()).thenReturn("Node");
    when(mockedEdge.getComponentTypeIdentifier()).thenReturn("Edge");

    while (!hadNode || !hadEdge) {
      GraphComponent randomComponent = randomGraphComponentRecorder.nextValue();

      assertNotNull(randomComponent);

      if (randomComponent.getComponentTypeIdentifier().equals(Edge.EDGE_IDENTIFIER)) {
        hadEdge = true;
      } else if (randomComponent.getComponentTypeIdentifier().equals(Node.NODE_IDENTIFIER)) {
        hadNode = true;
      }
    }
  }

  @Test
  public void testLastValue() {
    GraphComponent graphComponent = randomGraphComponentRecorder.nextValue();

    assertEquals(graphComponent, randomGraphComponentRecorder.lastValue());
  }

  private List<String> getLines(File file) throws IOException {
    return Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file)
        .getEncoding()));
  }

}