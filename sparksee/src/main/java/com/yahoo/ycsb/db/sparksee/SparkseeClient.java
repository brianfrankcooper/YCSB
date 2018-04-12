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

package com.yahoo.ycsb.db.sparksee;

import com.sparsity.sparksee.gdb.*;
import com.sparsity.sparksee.gdb.Objects;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Node;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sparksee client for the YCSB benchmark.
 */
public class SparkseeClient extends DB {

  static final String SPARKSEE_DATABASE_PATH_PROPERTY = "sparksee.path";
  private static final String SPARKSEE_DATABASE_PATH_DEFAULT = "sparkseeDB.gdb";
  private static final String SPARKSEE_LOG_LEVEL_PROPERTY = "sparksee.logLevel";
  private static final String SPARKSEE_LOG_LEVEL_DEFAULT = "Off";
  private static final Object INIT_LOCK = new Object();
  private static final AtomicInteger INIT_COUNT = new AtomicInteger();

  private static boolean initialised = false;
  private static Sparksee sparksee;
  private static Database database;
  private static Integer nodeIdAttribute = null;
  private static Integer edgeIdAttribute = null;

  @Override
  public void init() throws DBException {
    super.init();

    INIT_COUNT.incrementAndGet();

    synchronized (INIT_LOCK) {
      if (!initialised) {
        Properties properties = getProperties();

        String path = properties.getProperty(SPARKSEE_DATABASE_PATH_PROPERTY, SPARKSEE_DATABASE_PATH_DEFAULT);
        String logLevel = properties.getProperty(SPARKSEE_LOG_LEVEL_PROPERTY, SPARKSEE_LOG_LEVEL_DEFAULT);

        SparkseeConfig sparkseeConfig = new SparkseeConfig();
        setLogLevel(sparkseeConfig, logLevel);

        sparksee = new Sparksee(sparkseeConfig);

        try {
          if (new File(path).exists()) {
            database = sparksee.open(path, false);
          } else {
            database = sparksee.create(path, "SparkseeDB");
          }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }

        try (Session session = database.newSession()) {
          Graph graph = session.getGraph();

          nodeIdAttribute = getAttribute(graph, getNodeType(graph), "sparksee.nodeId");
          edgeIdAttribute = getAttribute(graph, getEdgeType(graph), "sparksee.edgeId");
        }

        initialised = true;
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      database.close();
      sparksee.close();
      initialised = false;
    }

    super.cleanup();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try (Session session = database.newSession()) {
      Graph graph = session.getGraph();

      long component;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        component = getEdge(graph, key);
      } else {
        component = getNode(graph, key);
      }

      addValuesToMap(graph, component, fields, result);
    } catch (RuntimeException e) {
      return Status.NOT_FOUND;
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    try (Session session = database.newSession()) {
      Graph graph = session.getGraph();

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        long edge = getEdge(graph, startkey);

        long startNode = graph.getEdgeData(edge).getTail();

        scanEdges(graph, startNode, recordcount, fields, result);
      } else {
        long node = getNode(graph, startkey);

        scanNodes(graph, node, recordcount, fields, result);
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try (Session session = database.newSession()) {
      Graph graph = session.getGraph();

      long component;
      int type;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        component = getEdge(graph, key);
        type = getEdgeType(graph);
      } else {
        component = getNode(graph, key);
        type = getNodeType(graph);
      }

      values.entrySet().forEach(entry -> setAttribute(graph, type, component, entry));
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try (Session session = database.newSession()) {
      Graph graph = session.getGraph();

      long component;
      int type;

      Value value = new Value();
      value.setString(key);

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        long startNode = getNode(graph, values.get(Edge.START_IDENTIFIER).toString());
        long endNode = getNode(graph, values.get(Edge.END_IDENTIFIER).toString());

        type = getEdgeType(graph);
        component = graph.newEdge(type, startNode, endNode);

        graph.setAttribute(component, edgeIdAttribute, value);
      } else {
        type = getNodeType(graph);
        component = graph.newNode(type);

        graph.setAttribute(component, nodeIdAttribute, value);
      }

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        setAttribute(graph, type, component, entry);
      }
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try (Session session = database.newSession()) {
      Graph graph = session.getGraph();

      long component;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        component = getEdge(graph, key);
      } else {
        component = getNode(graph, key);
      }

      graph.drop(component);
    }

    return Status.OK;
  }

  private void setLogLevel(SparkseeConfig sparkseeConfig, String logLevel) {
    boolean validLogLevel = validateLogLevel(logLevel);

    if (validLogLevel) {
      sparkseeConfig.setLogLevel(LogLevel.valueOf(logLevel));
    } else {
      sparkseeConfig.setLogLevel(LogLevel.valueOf(SPARKSEE_LOG_LEVEL_DEFAULT));
    }
  }

  private boolean validateLogLevel(String logLevel) {
    boolean validLogLevel = false;

    for (LogLevel level : LogLevel.values()) {
      if (level.name().equals(logLevel)) {
        validLogLevel = true;
      }
    }
    return validLogLevel;
  }

  private void addValuesToMap(Graph graph, long component, Set<String> fields, Map<String, ByteIterator> result) {
    HashMap<String, String> availableAttributes = getAvailableAttributeMap(graph, component);

    if (fields != null && !fields.isEmpty()) {
      fields.forEach(field -> {
        String availableValue = availableAttributes.get(field);
        if (availableValue != null) {
          result.put(field, new StringByteIterator(availableValue));
        }
      });
    } else {
      availableAttributes.forEach((attributeName, attributeValue) ->
          result.put(attributeName, new StringByteIterator(attributeValue)));
    }
  }

  private void scanEdges(Graph graph,
                         long component,
                         int recordcount,
                         Set<String> fields,
                         Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    HashMap<String, ByteIterator> values = new HashMap<>();

    try (Objects outgoingEdges = graph.explode(component, getEdgeType(graph), EdgesDirection.Outgoing)) {
      outgoingEdges.forEach(edge -> {
        addValuesToMap(graph, edge, fields, values);
        result.add(values);
        scanEdges(graph, graph.getEdgeData(edge).getHead(), recordcount, fields, result);
      });
    }
  }

  private void scanNodes(Graph graph,
                         long component,
                         int recordcount,
                         Set<String> fields,
                         Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    HashMap<String, ByteIterator> values = new HashMap<>();
    addValuesToMap(graph, component, fields, values);
    result.add(values);

    try (Objects neighbors = graph.neighbors(component, getEdgeType(graph), EdgesDirection.Outgoing)) {
      neighbors.forEach(neighbor -> scanNodes(graph, neighbor, recordcount, fields, result));
    }
  }

  private HashMap<String, String> getAvailableAttributeMap(Graph graph, long component) throws RuntimeException {
    AttributeList attributes = graph.getAttributes(component);

    HashMap<String, String> availableAttributes = new HashMap<>();

    attributes.iterator().forEachRemaining(attribute -> {
      String attributeName = graph.getAttribute(attribute).getName();
      String attributeValue = graph.getAttribute(component, attribute).getString();

      availableAttributes.put(attributeName, attributeValue);
    });

    return availableAttributes;
  }

  private long getNode(Graph graph, String key) {
    Value value = new Value();
    value.setString(key);
    return graph.findObject(nodeIdAttribute, value);
  }

  private long getEdge(Graph graph, String key) {
    Value value = new Value();
    value.setString(key);
    return graph.findObject(edgeIdAttribute, value);
  }

  private int getEdgeType(Graph graph) {
    return graph.findType(Edge.EDGE_IDENTIFIER) != Type.InvalidType
        ? graph.findType(Edge.EDGE_IDENTIFIER) : graph.newEdgeType(Edge.EDGE_IDENTIFIER, true, false);
  }

  private int getNodeType(Graph graph) {
    return graph.findType(Node.NODE_IDENTIFIER) != Type.InvalidType
        ? graph.findType(Node.NODE_IDENTIFIER) : graph.newNodeType(Node.NODE_IDENTIFIER);
  }

  private void setAttribute(Graph graph, int type, long oid, Map.Entry<String, ByteIterator> entry) {
    int attributeIdentifier = getAttribute(graph, type, entry.getKey());
    Value value = new Value();
    value.setString(entry.getValue().toString());
    graph.setAttribute(oid, attributeIdentifier, value);
  }

  private int getAttribute(Graph graph, int type, String key) {
    return graph.findAttribute(type, key) != Attribute.InvalidAttribute
        ? graph.findAttribute(type, key) : graph.newAttribute(type, key, DataType.String, AttributeKind.Basic);
  }

}
