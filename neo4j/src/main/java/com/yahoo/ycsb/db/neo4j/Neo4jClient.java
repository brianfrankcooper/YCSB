/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
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
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Neo4j client for YCSB framework.
 */
public class Neo4jClient extends DB {
  public static final String DEFAULT_PROP = "";
  /** Path used to create the database directory. */
  public static final String BASE_PATH = "db.path";
  /** Default path used to create the database directory, if no arguments are given. */
  public static final String DEFAULT_PATH = "neo4j.db";
  /** The name of the node identifier field. */
  private static final String NODE_ID = "_key";
  /** The graph database instance, initialized only once. */
  private static GraphDatabaseService graphDbInstance;
  /** Integer used to keep track of current threads. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Initializes the graph database, only once per DB instance.
   *
   * @throws DBException
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    // syncing all threads
    synchronized (Neo4jClient.class) {
      // instantiating graph db only once
      if (graphDbInstance == null) {
        String path = getProperties().getProperty(BASE_PATH, DEFAULT_PROP);
        if (path == null) {
          graphDbInstance = new GraphDatabaseFactory().newEmbeddedDatabase(new File(DEFAULT_PATH));
        } else {
          graphDbInstance = new GraphDatabaseFactory().newEmbeddedDatabase(new File(path));
        }
      }
    }
  }

  /**
   * Shuts down the Neo4j graph database, called once per DB instance.
   *
   * @throws DBException
   */
  @Override
  public void cleanup() throws DBException {
    // making sure that all threads are done working
    if (INIT_COUNT.decrementAndGet() == 0) {
      graphDbInstance.shutdown();
    }
  }

  /**
   * Reads a set of fields found in a labelled node.
   *
   * @param label  Table name
   * @param key    Record key of the node to read
   * @param fields Fields to read
   * @param result A Vector of HashMaps, where each HashMap is a set field/value
   *               pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String label, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    // starting transaction
    try (Transaction tx = graphDbInstance.beginTx()) {
      Node n = graphDbInstance.findNode(Label.label(label), NODE_ID, key);
      // searching for properties with fields names, and putting their values in result
      if (fields != null) {
        for (String field : fields) {
          result.put(field, new StringByteIterator((String) n.getProperty(field)));
        }
      }

      tx.success();
      return Status.OK;
    } catch (Exception e) {
      System.out.println(e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param label       Table name
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error.
   */
  @Override
  public Status scan(String label, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // starting transaction
    try (Transaction tx = graphDbInstance.beginTx()) {
      // finding nodes to scan
      Result cypherResult = graphDbInstance.execute("match (n:" + label + ") where n." + NODE_ID + " >= '"
              + startkey + "' return n limit " + recordcount);
      Iterator<Node> nColumn = cypherResult.columnAs("n");

      for (Node node : Iterators.asIterable(nColumn)) {
        HashMap<String, ByteIterator> nodeScanResult = new HashMap<String, ByteIterator>();

        // searching for properties with fields names, and putting their values in result
        if (fields != null) {
          for (String field : fields) {
            nodeScanResult.put(field, new StringByteIterator((String) node.getProperty(field)));
          }
        }
        result.add(nodeScanResult);
      }

      tx.success();
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * Updates a new node in the neo4j database. Any field/value pairs in the specified
   * values HashMap will be written into the node with the specified node
   * key, overwriting any existing values with the same property name.
   *
   * @param label  Table name
   * @param key    Node identifier
   * @param values Values to insert/update (key-value hashmap)
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String label, String key, HashMap<String, ByteIterator> values) {
    // starting transaction
    try (Transaction tx = graphDbInstance.beginTx()) {
      // finding node
      Node n = graphDbInstance.findNode(Label.label(label), NODE_ID, key);
      // updating/inserting values in node
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        n.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }

      tx.success();
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }

  }

  /**
   * Inserts a new node in the neo4j database. Any field/value pairs in the specified
   * values HashMap will be written into the node with the specified node
   * key.
   *
   * @param label  Table name
   * @param key    Node identifier
   * @param values Values to insert (key-value hashmap)
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String label, String key, HashMap<String, ByteIterator> values) {
    // starting transaction
    try (Transaction tx = graphDbInstance.beginTx()) {
      // inserting node and setting up identifier
      Node n = graphDbInstance.createNode(Label.label(label));
      n.setProperty(NODE_ID, key);
      // inserting values in current node
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        n.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }

      tx.success();
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * Deletes a node found by its label and property.
   *
   * @param label Label to find
   * @param key   Identifier property value
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String label, String key) {
    // starting transaction
    try (Transaction tx = graphDbInstance.beginTx()) {
      // inserting node and setting up identifier
      Node n = graphDbInstance.findNode(Label.label(label), NODE_ID, key);
      n.delete();
      tx.success();
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
}
