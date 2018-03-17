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
import com.yahoo.ycsb.generator.graph.GraphDataGenerator;
import com.yahoo.ycsb.generator.graph.Node;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRandomGraphComponentRecreator {

  private static RandomGraphComponentRecreator randomGraphComponentRecreator;
  private static String directory = "src" + File.separator + "test" + File.separator + "resources";
  private static GraphDataGenerator graphDataGenerator;

  @BeforeClass
  public static void setUp() throws IOException {
    graphDataGenerator = mock(GraphDataGenerator.class);
    Node mockedNode = mock(Node.class);
    Edge mockedEdge = mock(Edge.class);

    when(graphDataGenerator.getNode(anyLong())).thenReturn(mockedNode);
    when(graphDataGenerator.getEdge(anyLong())).thenReturn(mockedEdge);

    randomGraphComponentRecreator = new RandomGraphComponentRecreator(directory, graphDataGenerator);
  }

  @Test
  public void testLoadingOfFiles() throws IOException {
    randomGraphComponentRecreator = new RandomGraphComponentRecreator(directory, graphDataGenerator);
  }

  @Test(expected = IOException.class)
  public void testLoadingOfFilesWithNoFilesPresent() throws IOException {
    randomGraphComponentRecreator = new RandomGraphComponentRecreator(System.getProperty("user.dir"),
        graphDataGenerator);
  }

  @Test
  public void chooseRandomEdgeId() throws IOException {
    List<String> edgeLines = Files.readAllLines(randomGraphComponentRecreator.getEdgeFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getEdgeFile()).getEncoding()));

    for (String edgeLine : edgeLines) {
      Assert.assertEquals(Long.parseLong(edgeLine), randomGraphComponentRecreator.chooseRandomEdgeId());
    }
  }

  @Test
  public void chooseRandomNodeId() throws IOException {
    List<String> nodeLines = Files.readAllLines(randomGraphComponentRecreator.getNodeFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getNodeFile()).getEncoding()));

    for (String nodeLine : nodeLines) {
      Assert.assertEquals(Long.parseLong(nodeLine), randomGraphComponentRecreator.chooseRandomNodeId());
    }
  }

  @Test
  public void chooseRandomNodeOrEdgeId() throws IOException {
    List<String> components = Files.readAllLines(randomGraphComponentRecreator.getComponentFile().toPath(),
        Charset.forName(new FileReader(randomGraphComponentRecreator.getComponentFile()).getEncoding()));

    for (String component : components) {
      Assert.assertEquals(component, randomGraphComponentRecreator.randomNodeOrEdge().name());
    }
  }
}