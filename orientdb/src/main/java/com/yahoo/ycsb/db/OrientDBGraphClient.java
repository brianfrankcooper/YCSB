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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.graph.Edge;
import com.yahoo.ycsb.generator.graph.Node;

import java.util.*;
import java.util.stream.Collectors;

import static com.yahoo.ycsb.db.OrientDBClient.*;

/**
 * This class implements the Graph API of OrientDB as a binding for YCSB.
 */
public class OrientDBGraphClient extends DB {

  /**
   * Whether or not to use light weight edges in the data base.
   * Defaults to true.
   *
   * @see <a href=https://orientdb.com/docs/2.2.x/Lightweight-Edges.html>Lightweigt edges</a>
   */
  private static final String USE_LIGHTWEIGHT_EDGES_PROPERTY = "orientdb.uselightweightedges";
  private static final String USE_LIGHTWEIGHT_EDGES_DEFAULT = "true";
  private static final Object INIT_LOCK = new Object();
  private final String edgeLabelIdentifier = "edge_" + Edge.LABEL_IDENTIFIER;
  private final String nodeIdIdentifier = "node_" + Node.ID_IDENTIFIER;

  private OrientGraphFactory factory;
  private String edgeIdIdentifier = "edge_" + Edge.ID_IDENTIFIER;
  private boolean initialised = false;


  @Override
  public void init() throws DBException {
    super.init();

    synchronized (INIT_LOCK) {
      if (!initialised) {
        Properties properties = getProperties();
        int threadCount = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
        String url = properties.getProperty(URL_PROPERTY, URL_PROPERTY_DEFAULT);
        String userName = properties.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
        String password = properties.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);

        boolean useLightweightEdges = Boolean.parseBoolean(properties.getProperty(
            USE_LIGHTWEIGHT_EDGES_PROPERTY,
            USE_LIGHTWEIGHT_EDGES_DEFAULT));

        factory = new OrientGraphFactory(url, userName, password);
        factory.setupPool(1, threadCount);
        factory.setUseLightweightEdges(useLightweightEdges);

        initialised = true;
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    factory.close();
    super.cleanup();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    OrientGraph graph = factory.getTx();

    try {

      Map<String, Object> properties;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        if (graph.isUseLightweightEdges()) {
          return Status.FORBIDDEN;
        }

        OrientEdge edge = getEdgeById(graph, key);

        properties = edge.getProperties();

        replaceConflictingEdgeIdentifiersInSet(fields);
      } else {
        OrientVertex node = getNodeById(graph, key);

        properties = node.getProperties();

        replaceConflictingNodeIdentifiersInSet(fields);
      }

      addFieldsToMapAndReplaceConflictingIdentifiers(fields, properties, result);
    } catch (NoSuchElementException | NullPointerException e) {
      return Status.NOT_FOUND;
    } finally {
      graph.shutdown();
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    OrientGraph graph = factory.getTx();

    try {
      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        if (graph.isUseLightweightEdges()) {
          return Status.FORBIDDEN;
        }

        if (fields != null) {
          replaceConflictingEdgeIdentifiersInSet(fields);
        }

        OrientEdge edge = getEdgeById(graph, startkey);

        OrientVertex vertex = edge.getVertex(Direction.OUT);

        scanEdges(graph, vertex, recordcount, fields, result);
      } else {
        if (fields != null) {
          replaceConflictingNodeIdentifiersInSet(fields);
        }

        OrientVertex vertex = getNodeById(graph, startkey);

        scanNodes(graph, vertex, recordcount, fields, result);
      }
    } finally {
      graph.shutdown();
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    OrientGraph graph = factory.getTx();

    Map<String, String> stringValues = convertToStringStringMap(values);

    try {
      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        if (graph.isUseLightweightEdges()) {
          return Status.FORBIDDEN;
        }
        OrientEdge edge = getEdgeById(graph, key);

        replaceConflictingEdgeIdentifiersInMap(stringValues);

        edge.setProperties(stringValues);
      } else {
        OrientVertex vertex = getNodeById(graph, key);

        replaceConflictingNodeIdentifiersInMap(stringValues);

        vertex.setProperties(stringValues);
      }
    } finally {
      graph.shutdown();
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    OrientGraph graph = factory.getTx();

    Map<String, String> stringValues = convertToStringStringMap(values);

    try {
      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        OrientVertex outVertex = getNodeById(graph, values.get(Edge.START_IDENTIFIER).toString());
        OrientVertex inVertex = getNodeById(graph, values.get(Edge.END_IDENTIFIER).toString());

        OrientEdge orientEdge = graph.addEdge(key, outVertex, inVertex, Edge.EDGE_IDENTIFIER);

        replaceConflictingEdgeIdentifiersInMap(stringValues);

        if (!graph.isUseLightweightEdges()) {
          orientEdge.setProperties(stringValues);
        }
      } else {
        replaceConflictingNodeIdentifiersInMap(stringValues);

        graph.addVertex(key, stringValues);
      }

    } finally {
      graph.shutdown();
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    OrientGraph graph = factory.getTx();

    try {
      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        OrientEdge edge = getEdgeById(graph, key);
        graph.removeEdgeInternal(edge);
      } else {
        OrientVertex vertex = getNodeById(graph, key);
        graph.removeVertex(vertex);
      }

    } finally {
      graph.shutdown();
    }

    return Status.OK;
  }

  private OrientVertex getNodeById(OrientGraph graph, String id) {
    return graph.getVertex(graph.getVertices(nodeIdIdentifier, id).iterator().next().getId());
  }

  private OrientEdge getEdgeById(OrientGraph graph, String id) {
    return graph.getEdge(graph.getEdges(edgeIdIdentifier, id).iterator().next().getId());
  }

  private void scanNodes(OrientGraph graph,
                         OrientVertex vertex,
                         int recordcount,
                         Set<String> fields,
                         Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    HashMap<String, ByteIterator> values = new HashMap<>();
    addFieldsToMapAndReplaceConflictingIdentifiers(fields, vertex.getProperties(), values);
    result.add(values);

    Iterable<com.tinkerpop.blueprints.Edge> edges = vertex.getEdges(Direction.OUT);

    for (com.tinkerpop.blueprints.Edge edge : edges) {
      OrientVertex inVertex = graph.getEdge(edge.getId()).getVertex(Direction.IN);

      scanNodes(graph, inVertex, recordcount, fields, result);
    }

  }

  private void scanEdges(OrientGraph graph, OrientVertex vertex,
                         int recordcount,
                         Set<String> fields,
                         Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    Iterable<com.tinkerpop.blueprints.Edge> edges = vertex.getEdges(Direction.OUT);

    for (com.tinkerpop.blueprints.Edge edge : edges) {
      HashMap<String, ByteIterator> values = new HashMap<>();

      OrientEdge orientEdge = graph.getEdge(edge.getId());

      addFieldsToMapAndReplaceConflictingIdentifiers(fields, orientEdge.getProperties(), values);
      result.add(values);

      OrientVertex inVertex = orientEdge.getVertex(Direction.IN);

      scanEdges(graph, inVertex, recordcount, fields, result);
    }
  }

  private void addFieldsToMapAndReplaceConflictingIdentifiers(Set<String> fields,
                                                              Map<String, Object> properties,
                                                              Map<String, ByteIterator> values) {
    if (fields != null) {
      fields.forEach(field -> values.put(field, new StringByteIterator(properties.get(field).toString())));
    } else {
      properties.forEach((mapKey, value) -> values.put(mapKey, new StringByteIterator(value.toString())));
    }

    ByteIterator value = values.remove(edgeIdIdentifier);
    if (value != null) {
      values.put(Edge.ID_IDENTIFIER, value);
    }

    value = values.remove(edgeLabelIdentifier);
    if (value != null) {
      values.put(Edge.LABEL_IDENTIFIER, value);
    }

    value = values.remove(nodeIdIdentifier);
    if (value != null) {
      values.put(Node.ID_IDENTIFIER, value);
    }
  }

  private void replaceConflictingEdgeIdentifiersInMap(Map<String, String> stringValues) {
    String id = stringValues.remove(Edge.ID_IDENTIFIER);
    if (id != null) {
      stringValues.put(edgeIdIdentifier, id);
    }

    String label = stringValues.remove(Edge.LABEL_IDENTIFIER);
    if (label != null) {
      stringValues.put(edgeLabelIdentifier, label);
    }
  }

  private void replaceConflictingNodeIdentifiersInMap(Map<String, String> stringValues) {
    String id = stringValues.remove(Node.ID_IDENTIFIER);
    if (id != null) {
      stringValues.put(nodeIdIdentifier, id);
    }
  }

  private void replaceConflictingEdgeIdentifiersInSet(Set<String> fields) {
    if (fields.remove(Edge.ID_IDENTIFIER)) {
      fields.add(edgeIdIdentifier);
    }

    if (fields.remove(Edge.LABEL_IDENTIFIER)) {
      fields.add(edgeLabelIdentifier);
    }
  }

  private void replaceConflictingNodeIdentifiersInSet(Set<String> fields) {
    if (fields.remove(Node.ID_IDENTIFIER)) {
      fields.add(nodeIdIdentifier);
    }
  }

  private Map<String, String> convertToStringStringMap(Map<String, ByteIterator> values) {
    return values
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
  }
}
