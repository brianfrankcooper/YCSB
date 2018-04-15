/*
 * Copyright (c) 2016 - 2018 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db.neo4j;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.graph.Edge;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yahoo.ycsb.Status.*;

/**
 * Neo4j client for YCSB framework.
 */
public class Neo4jClient extends DB {
  /**
   * Path used to create the database directory.
   */
  static final String BASE_PATH_PROPERTY = "neo4j.path";
  static final String USE_INDEX_PROPERTY = "neo4j.index";

  private static final String BASE_PATH_DEFAULT = "neo4j.db";
  private static final String USE_INDEX_DEFAULT = "false";

  /**
   * The name of the node identifier field.
   */
  private static final String NODE_ID = "node_id";
  private static final String RELATIONSHIP_ID = "relationship_id";
  /**
   * Integer used to keep track of current threads.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger();
  /**
   * The graph database instance, initialized only once.
   */
  private static GraphDatabaseService graphDbInstance;

  private static Index<Node> nodeIndex;
  private static Index<Relationship> relationshipIndex;
  private boolean useIndex;

  @Override
  public void init() {
    INIT_COUNT.incrementAndGet();

    synchronized (Neo4jClient.class) {
      if (graphDbInstance == null) {
        String path = getProperties().getProperty(BASE_PATH_PROPERTY, BASE_PATH_DEFAULT);
        useIndex = Boolean.parseBoolean(getProperties().getProperty(USE_INDEX_PROPERTY, USE_INDEX_DEFAULT));

        graphDbInstance = new GraphDatabaseFactory().newEmbeddedDatabase(new File(path));

        if (useIndex) {
          try (Transaction transaction = graphDbInstance.beginTx()) {
            IndexManager index = graphDbInstance.index();
            nodeIndex = index.forNodes("nodes");
            relationshipIndex = index.forRelationships("relationships");
            transaction.success();
          }
        }
      }
    }
  }

  @Override
  public void cleanup() {
    if (INIT_COUNT.decrementAndGet() == 0) {
      graphDbInstance.shutdown();
      graphDbInstance = null;
    }
  }

  /**
   * Reads a set of fields found in a graph component with the {@link Label}/{@link RelationshipType} "key".
   *
   * @param table  used to distinguish between Relationships and Nodes.
   * @param key    to find the component.
   * @param fields to read. If null all properties will be read.
   * @param result a map to store the found properties in.
   * @return {@link Status}.OK if everything was found, Status.NOT_FOUND if the component couldn't be found for the
   * given key and Status.ERROR if an exception occurs.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try (Transaction tx = graphDbInstance.beginTx()) {
      Optional<? extends PropertyContainer> container;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        container = getRelationship(key);
      } else {
        container = getNode(key);
      }

      if (!container.isPresent()) {
        return NOT_FOUND;
      }

      insertFieldsIntoValueMap(fields, container.get().getAllProperties(), result);

      tx.success();
    } catch (NotFoundException e) {
      return NOT_FOUND;
    } catch (Exception e) {
      return ERROR;
    }

    return OK;
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   * The graph will be scanned in a depth first manner.
   *
   * @param table       used to distinguish between Relationships and Nodes.
   * @param startkey    is the key of the first record to read.
   * @param recordcount number of records to read.
   * @param fields      list of fields to read, or null for all of them.
   * @param result      a {@link Vector} of {@link HashMap}s, where each HashMap is a set field/value
   *                    pairs for one record.
   * @return {@link Status}.OK if everything was found, Status.NOT_FOUND if the component couldn't be found for the
   * given key and Status.ERROR if an exception occurs.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (Transaction tx = graphDbInstance.beginTx()) {

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Optional<Relationship> optionalRelationship = getRelationship(startkey);

        if (!optionalRelationship.isPresent()) {
          return NOT_FOUND;
        }

        Node startNode = optionalRelationship.get().getStartNode();

        scanEdges(startNode, recordcount, fields, result);
      } else {
        Optional<Node> optionalNode = getNode(startkey);

        if (!optionalNode.isPresent()) {
          return NOT_FOUND;
        }

        Node node = optionalNode.get();

        scanNodes(node, recordcount, fields, result);
      }

      tx.success();
    } catch (Exception e) {
      return ERROR;
    }

    return OK;
  }

  /**
   * Updates a {@link PropertyContainer} in the neo4j database.
   * All field/value pairs in the specified values {@link Map} will be written into the container with the specified
   * key, overwriting any existing values with the same property name.
   *
   * @param table  used to distinguish between Relationships and Nodes.
   * @param key    to identify the {@link Node} or {@link Relationship}.
   * @param values to insert/update.
   * @return {@link Status}.OK if everything was found, Status.NOT_FOUND if the component couldn't be found for the
   * given key and Status.ERROR if an exception occurs.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try (Transaction tx = graphDbInstance.beginTx()) {
      Optional<? extends PropertyContainer> container;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        container = getRelationship(key);
      } else {
        container = getNode(key);
      }

      if (!container.isPresent()) {
        return NOT_FOUND;
      }

      setProperties(container.get(), values);

      tx.success();
    } catch (Exception e) {
      return ERROR;
    }

    return OK;
  }

  /**
   * Inserts a new graph component ({@link Node} or {@link Relationship}) in the neo4j database.
   * Any key/value pairs in the specified values Map will be written into the component with the specified key.
   *
   * @param table  used to distinguish between Relationships and Nodes.
   * @param key    used as {@link Label} or {@link RelationshipType}.
   * @param values to set as properties.
   * @return {@link Status}.OK if everything was found, Status.BAD_REQUEST if a {@link Relationship} should be
   * inserted but on of the two corresponding {@link Node}s is not present and Status.ERROR if an exception occurs.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try (Transaction tx = graphDbInstance.beginTx()) {
      PropertyContainer container;

      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Node startNode = graphDbInstance.findNodes(Label.label(values.get(Edge.START_IDENTIFIER).toString())).next();
        Node endNode = graphDbInstance.findNodes(Label.label(values.get(Edge.END_IDENTIFIER).toString())).next();

        if (startNode == null || endNode == null) {
          return BAD_REQUEST;
        }

        RelationshipType relationshipType = RelationshipType.withName(key);

        startNode.createRelationshipTo(endNode, relationshipType);
        Relationship edge = startNode.getSingleRelationship(relationshipType, Direction.OUTGOING);

        if (useIndex) {
          relationshipIndex.add(edge, RELATIONSHIP_ID, key);
        }

        container = edge;
      } else {
        Node node = graphDbInstance.createNode(Label.label(key));

        if (useIndex) {
          nodeIndex.add(node, NODE_ID, key);
        }

        node.setProperty(NODE_ID, key);
        container = node;
      }

      setProperties(container, values);

      tx.success();
    } catch (Exception e) {
      return ERROR;
    }

    return OK;
  }

  /**
   * Deletes a graph component ({@link Node} or {@link Relationship}) with the given key.
   *
   * @param table used to distinguish between Relationships and Nodes.
   * @param key   to identify the {@link PropertyContainer}.
   * @return {@link Status}.OK if everything was found, Status.NOT_FOUND if the component couldn't be found for the
   * given key and Status.ERROR if an exception occurs.
   */
  @Override
  public Status delete(String table, String key) {
    try (Transaction tx = graphDbInstance.beginTx()) {
      if (table.equals(Edge.EDGE_IDENTIFIER)) {
        Optional<Relationship> relationship = getRelationship(key);

        if (!relationship.isPresent()) {
          return NOT_FOUND;
        }

        if (useIndex) {
          relationshipIndex.remove(relationship.get());
        }

        relationship.get().delete();
      } else {
        Optional<Node> node = getNode(key);

        if (!node.isPresent()) {
          return NOT_FOUND;
        }

        if (useIndex) {
          nodeIndex.remove(node.get());
        }

        node.get().delete();
      }

      tx.success();
    } catch (Exception e) {
      return ERROR;
    }

    return OK;
  }

  private void scanNodes(Node node, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    HashMap<String, ByteIterator> values = new HashMap<>();

    insertFieldsIntoValueMap(fields, node.getAllProperties(), values);
    result.add(values);

    Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

    relationships.forEach(relationship -> scanNodes(relationship.getEndNode(), recordcount, fields, result));
  }

  private void scanEdges(Node startNode,
                         int recordcount,
                         Set<String> fields,
                         Vector<HashMap<String, ByteIterator>> result) {
    if (result.size() >= recordcount) {
      return;
    }

    Iterable<Relationship> relationships = startNode.getRelationships(Direction.OUTGOING);

    for (Relationship relationship : relationships) {
      HashMap<String, ByteIterator> values = new HashMap<>();

      insertFieldsIntoValueMap(fields, relationship.getAllProperties(), values);
      result.add(values);

      scanEdges(relationship.getEndNode(), recordcount, fields, result);
    }
  }

  private void insertFieldsIntoValueMap(Set<String> fields,
                                        Map<String, Object> properties,
                                        Map<String, ByteIterator> values) {
    if (fields != null) {
      fields.forEach(field -> values.put(field, new StringByteIterator(properties.get(field).toString())));
    } else {
      properties.forEach((key, value) -> values.put(key, new StringByteIterator(value.toString())));
    }
  }

  private Optional<Node> getNode(String key) {
    Node node;

    if (useIndex) {
      node = nodeIndex.get(NODE_ID, key).getSingle();
    } else {
      node = graphDbInstance.findNode(Label.label(key), NODE_ID, key);
    }

    if (node == null) {
      return Optional.empty();
    }

    return Optional.of(node);
  }

  private Optional<Relationship> getRelationship(String key) {
    if (useIndex) {
      return Optional.of(relationshipIndex.get(RELATIONSHIP_ID, key).getSingle());
    } else {
      return graphDbInstance
          .getAllRelationships()
          .stream()
          .filter(relationship -> relationship.getType().name().equals(key))
          .findFirst();
    }
  }

  private void setProperties(PropertyContainer container, Map<String, ByteIterator> values) {
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      container.setProperty(entry.getKey(), entry.getValue().toString());
    }
  }
}
