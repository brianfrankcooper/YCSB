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

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Node;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGraphWorkload {

  private static DB db;
  private static Status status;
  private static GraphWorkload graphWorkload;
  private String directory = System.getProperty("user.dir") + File.separator + "test";
  private Properties properties;
  private int lastNodeLoadId = 14;
  private int lastEdgeLoadId = 16;

  @BeforeClass
  public static void setUpMock() {
    db = mock(DB.class);
    status = mock(Status.class);
  }

  @Before
  public void setUpTestFolder() {
    File file = new File(directory);
    file.mkdirs();
  }

  @After
  public void tearDownTestFolder() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Before
  public void resetGraphWorkload() {
    graphWorkload = new GraphWorkload();
  }

  @Before
  public void resetProperties() {
    properties = new Properties();
    properties.setProperty(GraphWorkload.DATA_SET_DIRECTORY_PROPERTY, directory);
  }

  @Before
  public void resetDbOpsToSucceed() {
    when(status.isOk()).thenReturn(true);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);
  }

  @Test
  public void doInsertFailing() throws WorkloadException {
    when(status.isOk()).thenReturn(false);
    when(db.insert(anyString(), anyString(), anyMap())).thenReturn(status);

    graphWorkload.init(properties);

    assertFalse(graphWorkload.doInsert(db, new Object()));
  }

  @Test
  public void doInsertSuccessful() throws WorkloadException {
    graphWorkload.init(properties);

    assertTrue(graphWorkload.doInsert(db, new Object()));
  }

  @Test
  public void testModeGenerateDataAndLoadPhaseFilesCreated() throws WorkloadException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "false");

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doInsert(db, new Object());

    HashSet<String> filenames = new HashSet<>(Arrays.asList(Objects.requireNonNull(new File(directory).list())));

    checkLoadFilesPresentAndRunFilesAbsent(filenames);
  }

  @Test
  public void testModeGenerateDataAndRunPhaseFilesCreated() throws WorkloadException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());

    HashSet<String> filenames = new HashSet<>(Arrays.asList(Objects.requireNonNull(new File(directory).list())));

    checkRunFilesPresentAndLoadFilesAbsent(filenames);
  }

  @Test
  public void testModeRunBenchmarkAndLoadPhase() throws WorkloadException, IOException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "false");

    createLoadPhaseFiles();

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doInsert(db, new Object());

    HashSet<String> filenames = new HashSet<>(Arrays.asList(Objects.requireNonNull(new File(directory).list())));

    checkLoadFilesPresentAndRunFilesAbsent(filenames);
  }

  @Test
  public void testModeRunBenchmarkAndRunPhaseWithoutLoadFilesPresent() throws WorkloadException, IOException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");

    createRunPhaseFiles();

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());

    HashSet<String> filenames = new HashSet<>(Arrays.asList(Objects.requireNonNull(new File(directory).list())));

    checkRunFilesPresentAndLoadFilesAbsent(filenames);
  }

  @Test
  public void testModeRunBenchmarkAndRunPhaseWithLoadFilesPresent() throws WorkloadException, IOException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "true");

    createLoadPhaseFilesAndSetLastIds();
    createRunPhaseFiles();

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());

    HashSet<String> filenames = new HashSet<>(Arrays.asList(Objects.requireNonNull(new File(directory).list())));

    assertTrue(filenames.contains(Node.NODE_IDENTIFIER + "load.json"));
    assertTrue(filenames.contains(Edge.EDGE_IDENTIFIER + "load.json"));
    assertTrue(filenames.contains(Node.NODE_IDENTIFIER + "run.json"));
    assertTrue(filenames.contains(Edge.EDGE_IDENTIFIER + "run.json"));
    assertTrue(filenames.contains("nodeIds.txt"));
    assertTrue(filenames.contains("operations.txt"));
    assertTrue(filenames.contains("edgeIds.txt"));
    assertTrue(filenames.contains("componentIds.txt"));
  }

  @Test(expected = NullPointerException.class)
  public void testModeGenerateDataAndLoadPhaseFailWithRunPhaseOperation() throws WorkloadException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "false");

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());
  }

  @Test(expected = NullPointerException.class)
  public void testModeRunBenchmarkAndLoadPhaseFailWithoutNecessaryFilesPresent() throws WorkloadException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "false");

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());
  }

  @Test(expected = NullPointerException.class)
  public void testModeRunBenchmarkAndRunPhaseFailWithoutNecessaryFilesPresent() throws WorkloadException {
    properties.setProperty(Client.DO_TRANSACTIONS_PROPERTY, "false");

    graphWorkload = new GraphWorkload();
    graphWorkload.init(properties);

    graphWorkload.doTransaction(db, new Object());
  }

  private void createRunPhaseFiles() throws IOException {
    new File(directory, Node.NODE_IDENTIFIER + "run.json").createNewFile();
    new File(directory, Edge.EDGE_IDENTIFIER + "run.json").createNewFile();
    new File(directory, "nodeIds.txt").createNewFile();
    new File(directory, "edgeIds.txt").createNewFile();
    new File(directory, "componentIds.txt").createNewFile();
    new File(directory, "operations.txt").createNewFile();
  }

  private void createLoadPhaseFiles() throws IOException {
    File nodeFile = new File(directory, Node.NODE_IDENTIFIER + "load.json");
    File edgeFile = new File(directory, Edge.EDGE_IDENTIFIER + "load.json");

    edgeFile.createNewFile();
    nodeFile.createNewFile();
  }

  private void createLoadPhaseFilesAndSetLastIds() throws IOException {
    File nodeFile = new File(directory, Node.NODE_IDENTIFIER + "load.json");
    File edgeFile = new File(directory, Edge.EDGE_IDENTIFIER + "load.json");

    edgeFile.createNewFile();
    nodeFile.createNewFile();

    FileWriter fileWriter = new FileWriter(nodeFile);
    fileWriter.write("{\"id\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"" + lastNodeLoadId + "\",\"off\":0}},\"" +
        "label\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"Factory\",\"off\":0}},\"" +
        "value\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"\\u0026Ys+Ck0\\u0027j3.\\u0026)\\u0026.1Je;)7\",\"off\":0}}}");
    fileWriter.close();

    fileWriter = new FileWriter(edgeFile);
    fileWriter.write("{\"start\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"" + lastNodeLoadId + "\",\"off\":0}},\"" +
        "end\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"" + lastNodeLoadId + "\",\"off\":0}},\"" +
        "id\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"" + lastEdgeLoadId + "\",\"off\":0}},\"" +
        "label\":{\"" +
        "type\":\"com.yahoo.ycsb.StringByteIterator\",\"" +
        "properties\":{\"str\":\"produced\",\"off\":0}}}");
    fileWriter.close();
  }

  private void checkLoadFilesPresentAndRunFilesAbsent(HashSet<String> filenames) {
    assertTrue(filenames.contains(Node.NODE_IDENTIFIER + "load.json"));
    assertTrue(filenames.contains(Edge.EDGE_IDENTIFIER + "load.json"));

    assertFalse(filenames.contains(Node.NODE_IDENTIFIER + "run.json"));
    assertFalse(filenames.contains(Edge.EDGE_IDENTIFIER + "run.json"));
    assertFalse(filenames.contains("nodeIds.txt"));
    assertFalse(filenames.contains("operations.txt"));
    assertFalse(filenames.contains("edgeIds.txt"));
    assertFalse(filenames.contains("componentIds.txt"));
  }

  private void checkRunFilesPresentAndLoadFilesAbsent(HashSet<String> filenames) {
    assertFalse(filenames.contains(Node.NODE_IDENTIFIER + "load.json"));
    assertFalse(filenames.contains(Edge.EDGE_IDENTIFIER + "load.json"));

    assertTrue(filenames.contains(Node.NODE_IDENTIFIER + "run.json"));
    assertTrue(filenames.contains(Edge.EDGE_IDENTIFIER + "run.json"));
    assertTrue(filenames.contains("nodeIds.txt"));
    assertTrue(filenames.contains("operations.txt"));
    assertTrue(filenames.contains("edgeIds.txt"));
    assertTrue(filenames.contains("componentIds.txt"));
  }
}
