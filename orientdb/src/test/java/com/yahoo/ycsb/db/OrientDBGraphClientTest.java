/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Node;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static org.junit.Assert.assertEquals;

public class OrientDBGraphClientTest {

  private static OrientDBGraphClient orientDBGraphClient;
  private static File directory = new File(System.getProperty("user.dir"), "test");
  private static int counter = 0;

  private final String firstNodeId = "0";
  private final String firstNodeLabel = "First";
  private final String firstNodeValue = "foo";
  private final String updatedFirstNodeLabel = "UpdatedFirst";
  private final String updatedFirstNodeValue = "fooOO";
  private final String secondNodeId = "1";
  private final String secondNodeLabel = "Second";
  private final String secondNodeValue = "bar";
  private final String updatedSecondNodeLabel = "UpdatedSecond";
  private final String updatedSecondNodeValue = "fooBar";
  private final String thirdNodeId = "3";
  private final String thirdNodeLabel = "Third";
  private final String thirdNodeValue = "'amThird";
  private final String fourthNodeId = "4";
  private final String fourthNodeLabel = "Fourth";
  private final String fourthNodeValue = "blop";
  private final String firstEdgeId = "0";
  private final String firstEdgeLabel = "EdgeLabel";
  private final String updatedEdgeLabel = "UpdatedEdgeLabel";
  private final String secondEdgeId = "1";
  private final String secondEdgeLabel = "OtherEdgeLabel";
  private final String thirdEdgeId = "3";
  private final String thirdEdgeLabel = "thirdEdgeLabel";

  @AfterClass
  public static void removeLastFolder() throws IOException {
    FileUtils.deleteDirectory(directory);
  }

  @Before
  public void setUpClientForNextTest() throws DBException, IOException {
    FileUtils.deleteDirectory(directory);

    directory = new File(System.getProperty("user.dir"), "test" + counter++);

    Properties properties = new Properties();
    properties.remove(OrientDBClient.URL_PROPERTY);
    properties.setProperty(OrientDBClient.URL_PROPERTY, "plocal:" + directory.getAbsolutePath());
    properties.setProperty("orientdb.uselightweightedges", "false");

    orientDBGraphClient = new OrientDBGraphClient();
    orientDBGraphClient.setProperties(properties);

    orientDBGraphClient.init();
  }

  @Test
  public void insertNodeAndEdge() {
    insertNode(firstNodeId, firstNodeLabel, firstNodeValue);
    insertNode(secondNodeId, secondNodeLabel, secondNodeValue);
    insertEdge(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId);
    insertEdge(secondEdgeId, secondEdgeLabel, firstNodeId, secondNodeId);
  }

  @Test
  public void update() {
    insertNode(firstNodeId, firstNodeLabel, firstNodeValue);
    insertNode(secondNodeId, secondNodeLabel, secondNodeValue);

    insertEdge(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId);

    updateNode(firstNodeId, updatedFirstNodeLabel, updatedFirstNodeValue);
    updateNode(secondNodeId, updatedSecondNodeLabel, updatedSecondNodeValue);

    updateEdge(firstEdgeId, updatedEdgeLabel, firstNodeId, secondNodeId);

    readNode(firstNodeId, updatedFirstNodeLabel, updatedFirstNodeValue);
    readNode(secondNodeId, updatedSecondNodeLabel, updatedSecondNodeValue);

    readEdge(firstEdgeId, updatedEdgeLabel, firstNodeId, secondNodeId);
  }

  @Test
  public void read() {
    insertNode(firstNodeId, firstNodeLabel, firstNodeValue);
    insertNode(secondNodeId, secondNodeLabel, secondNodeValue);

    insertEdge(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId);

    readNode(firstNodeId, firstNodeLabel, firstNodeValue);
    readNode(secondNodeId, secondNodeLabel, secondNodeValue);

    readEdge(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId);

    assertEquals(Status.NOT_FOUND, orientDBGraphClient.read(Node.NODE_IDENTIFIER, "6", null, new HashMap<>()));
  }

  @Test
  public void scan() {
    insertNode(firstNodeId, firstNodeLabel, firstNodeValue);
    insertNode(secondNodeId, secondNodeLabel, secondNodeValue);
    insertNode(thirdNodeId, thirdNodeLabel, thirdNodeValue);
    insertNode(fourthNodeId, fourthNodeLabel, fourthNodeValue);

    insertEdge(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId);
    insertEdge(secondEdgeId, secondEdgeLabel, secondNodeId, thirdNodeId);
    insertEdge(thirdEdgeId, thirdEdgeLabel, thirdNodeId, fourthNodeId);

    Vector<HashMap<String, ByteIterator>> results = new Vector<>();

    assertEquals(Status.OK, orientDBGraphClient.scan(Node.NODE_IDENTIFIER,
        firstNodeId,
        4,
        Node.NODE_FIELDS_SET,
        results));
    assertEquals(4, results.size());
    checkNodeValues(firstNodeId, firstNodeLabel, firstNodeValue, results.get(0));
    checkNodeValues(secondNodeId, secondNodeLabel, secondNodeValue, results.get(1));
    checkNodeValues(thirdNodeId, thirdNodeLabel, thirdNodeValue, results.get(2));
    checkNodeValues(fourthNodeId, fourthNodeLabel, fourthNodeValue, results.get(3));

    results.clear();

    assertEquals(Status.OK, orientDBGraphClient.scan(Node.NODE_IDENTIFIER,
        firstNodeId,
        2,
        Node.NODE_FIELDS_SET,
        results));
    assertEquals(2, results.size());
    checkNodeValues(firstNodeId, firstNodeLabel, firstNodeValue, results.get(0));
    checkNodeValues(secondNodeId, secondNodeLabel, secondNodeValue, results.get(1));

    results.clear();

    assertEquals(Status.OK, orientDBGraphClient.scan(Node.NODE_IDENTIFIER,
        firstNodeId,
        4,
        null,
        results));
    assertEquals(4, results.size());
    checkNodeValues(firstNodeId, firstNodeLabel, firstNodeValue, results.get(0));
    checkNodeValues(secondNodeId, secondNodeLabel, secondNodeValue, results.get(1));
    checkNodeValues(thirdNodeId, thirdNodeLabel, thirdNodeValue, results.get(2));
    checkNodeValues(fourthNodeId, fourthNodeLabel, fourthNodeValue, results.get(3));

    results.clear();

    assertEquals(Status.OK, orientDBGraphClient.scan(Edge.EDGE_IDENTIFIER,
        firstEdgeId,
        3,
        Edge.EDGE_FIELDS_SET,
        results));
    assertEquals(3, results.size());
    checkEdgeValues(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId, results.get(0));
    checkEdgeValues(secondEdgeId, secondEdgeLabel, secondNodeId, thirdNodeId, results.get(1));
    checkEdgeValues(thirdEdgeId, thirdEdgeLabel, thirdNodeId, fourthNodeId, results.get(2));

    results.clear();

    assertEquals(Status.OK, orientDBGraphClient.scan(Edge.EDGE_IDENTIFIER,
        firstEdgeId,
        3,
        null,
        results));
    assertEquals(3, results.size());
    checkEdgeValues(firstEdgeId, firstEdgeLabel, firstNodeId, secondNodeId, results.get(0));
    checkEdgeValues(secondEdgeId, secondEdgeLabel, secondNodeId, thirdNodeId, results.get(1));
    checkEdgeValues(thirdEdgeId, thirdEdgeLabel, thirdNodeId, fourthNodeId, results.get(2));
  }

  @Test
  public void delete() {
    insertNode(firstNodeId, firstNodeLabel, firstNodeValue);

    assertEquals(Status.OK, orientDBGraphClient.delete(Node.NODE_IDENTIFIER, firstNodeId));

    assertEquals(Status.NOT_FOUND, orientDBGraphClient.read(Node.NODE_IDENTIFIER, firstNodeId, null, new HashMap<>()));
  }

  @Test
  public void cleanup() throws DBException {
    orientDBGraphClient.cleanup();
  }

  private void readNode(String nodeId, String nodeLabel, String nodeValue) {
    Map<String, ByteIterator> result = new HashMap<>();

    assertEquals(Status.OK, orientDBGraphClient.read(Node.NODE_IDENTIFIER, nodeId, Node.NODE_FIELDS_SET, result));
    assertEquals(nodeId, result.get(Node.ID_IDENTIFIER).toString());
    assertEquals(nodeLabel, result.get(Node.LABEL_IDENTIFIER).toString());
    assertEquals(nodeValue, result.get(Node.VALUE_IDENTIFIER).toString());
  }

  private void readEdge(String edgeId, String edgeLabel, String startNodeId, String endNodeId) {
    Map<String, ByteIterator> result = new HashMap<>();

    assertEquals(Status.OK, orientDBGraphClient.read(Edge.EDGE_IDENTIFIER, edgeId, Edge.EDGE_FIELDS_SET, result));
    assertEquals(edgeId, result.get(Edge.ID_IDENTIFIER).toString());
    assertEquals(edgeLabel, result.get(Edge.LABEL_IDENTIFIER).toString());
    assertEquals(startNodeId, result.get(Edge.START_IDENTIFIER).toString());
    assertEquals(endNodeId, result.get(Edge.END_IDENTIFIER).toString());
  }

  private void updateNode(String nodeId, String label, String value) {
    assertEquals(Status.OK, orientDBGraphClient.update(Node.NODE_IDENTIFIER,
        nodeId,
        getNodeMap(nodeId, label, value)));
  }

  private void updateEdge(String edgeId, String edgeLabel, String startNodeId, String endNodeId) {
    assertEquals(Status.OK, orientDBGraphClient.update(Edge.EDGE_IDENTIFIER,
        edgeId,
        getEdgeMap(edgeId, edgeLabel, startNodeId, endNodeId)));
  }

  private void insertNode(String nodeId, String label, String value) {
    assertEquals(Status.OK, orientDBGraphClient.insert(Node.NODE_IDENTIFIER,
        nodeId,
        getNodeMap(nodeId, label, value)));
  }

  private void insertEdge(String edgeId, String label, String startNodeId, String endNodeId) {
    assertEquals(Status.OK, orientDBGraphClient.insert(Edge.EDGE_IDENTIFIER,
        edgeId,
        getEdgeMap(edgeId, label, startNodeId, endNodeId)));
  }

  private Map<String, ByteIterator> getEdgeMap(String id, String label, String start, String end) {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put(Edge.ID_IDENTIFIER, new StringByteIterator(id));
    values.put(Edge.LABEL_IDENTIFIER, new StringByteIterator(label));
    values.put(Edge.START_IDENTIFIER, new StringByteIterator(start));
    values.put(Edge.END_IDENTIFIER, new StringByteIterator(end));
    return values;
  }

  private Map<String, ByteIterator> getNodeMap(String id, String label, String value) {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put(Node.ID_IDENTIFIER, new StringByteIterator(id));
    values.put(Node.LABEL_IDENTIFIER, new StringByteIterator(label));
    values.put(Node.VALUE_IDENTIFIER, new StringByteIterator(value));
    return values;
  }

  private void checkEdgeValues(String edgeId, String edgeLabel, String startNodeId, String endNodeId,
                               HashMap<String, ByteIterator> edgeValues) {
    assertEquals(edgeId, edgeValues.get(Edge.ID_IDENTIFIER).toString());
    assertEquals(edgeLabel, edgeValues.get(Edge.LABEL_IDENTIFIER).toString());
    assertEquals(startNodeId, edgeValues.get(Edge.START_IDENTIFIER).toString());
    assertEquals(endNodeId, edgeValues.get(Edge.END_IDENTIFIER).toString());
  }

  private void checkNodeValues(String nodeId, String nodeLabel, String nodeValue, HashMap<String, ByteIterator> nodeValues) {
    assertEquals(nodeId, nodeValues.get(Node.ID_IDENTIFIER).toString());
    assertEquals(nodeLabel, nodeValues.get(Node.LABEL_IDENTIFIER).toString());
    assertEquals(nodeValue, nodeValues.get(Node.VALUE_IDENTIFIER).toString());
  }
}
