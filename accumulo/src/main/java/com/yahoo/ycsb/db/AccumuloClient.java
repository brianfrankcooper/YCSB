/**
 * Copyright (c) 2011 YCSB++ project, 2014 YCSB contributors.
 * All rights reserved.
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

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CleanUp;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/**
 * <a href="https://accumulo.apache.org/">Accumulo</a> binding for YCSB.
 */
public class AccumuloClient extends DB {

  private ZooKeeperInstance inst;
  private Connector connector;
  private String table = "";
  private BatchWriter bw = null;
  private Text colFam = new Text("");
  private Scanner singleScanner = null; // A scanner for reads/deletes.
  private Scanner scanScanner = null; // A scanner for use by scan()

  private static final String PC_PRODUCER = "producer";
  private static final String PC_CONSUMER = "consumer";
  private String pcFlag = "";
  private ZKProducerConsumer.Queue q = null;
  private static Hashtable<String, Long> hmKeyReads = null;
  private static Hashtable<String, Integer> hmKeyNumReads = null;
  private Random r = null;

  @Override
  public void init() throws DBException {
    colFam = new Text(getProperties().getProperty("accumulo.columnFamily"));

    inst = new ZooKeeperInstance(
        getProperties().getProperty("accumulo.instanceName"),
        getProperties().getProperty("accumulo.zooKeepers"));
    try {
      String principal = getProperties().getProperty("accumulo.username");
      AuthenticationToken token =
          new PasswordToken(getProperties().getProperty("accumulo.password"));
      connector = inst.getConnector(principal, token);
    } catch (AccumuloException e) {
      throw new DBException(e);
    } catch (AccumuloSecurityException e) {
      throw new DBException(e);
    }

    pcFlag = getProperties().getProperty("accumulo.PC_FLAG", "none");
    if (pcFlag.equals(PC_PRODUCER) || pcFlag.equals(PC_CONSUMER)) {
      System.out.println("*** YCSB Client is " + pcFlag);
      String address = getProperties().getProperty("accumulo.PC_SERVER");
      String root = getProperties().getProperty("accumulo.PC_ROOT_IN_ZK");
      System.out
          .println("*** PC_INFO(server:" + address + ";root=" + root + ")");
      q = new ZKProducerConsumer.Queue(address, root);
      r = new Random();
    }

    if (pcFlag.equals(PC_CONSUMER)) {
      hmKeyReads = new Hashtable<String, Long>();
      hmKeyNumReads = new Hashtable<String, Integer>();
      try {
        keyNotification(null);
      } catch (KeeperException e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (bw != null) {
        bw.close();
      }
    } catch (MutationsRejectedException e) {
      throw new DBException(e);
    }
    CleanUp.shutdownNow();
  }

  /**
   * Commonly repeated functionality: Before doing any operation, make sure
   * we're working on the correct table. If not, open the correct one.
   * 
   * @param t
   *          The table to open.
   */
  public void checkTable(String t) throws TableNotFoundException {
    if (!table.equals(t)) {
      getTable(t);
    }
  }

  /**
   * Called when the user specifies a table that isn't the same as the existing
   * table. Connect to it and if necessary, close our current connection.
   * 
   * @param t
   *          The table to open.
   */
  public void getTable(String t) throws TableNotFoundException {
    if (bw != null) { // Close the existing writer if necessary.
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        // Couldn't spit out the mutations we wanted.
        // Ignore this for now.
        System.err.println("MutationsRejectedException: " + e.getMessage());
      }
    }

    BatchWriterConfig bwc = new BatchWriterConfig();
    bwc.setMaxLatency(
        Long.parseLong(getProperties()
            .getProperty("accumulo.batchWriterMaxLatency", "30000")),
        TimeUnit.MILLISECONDS);
    bwc.setMaxMemory(Long.parseLong(
        getProperties().getProperty("accumulo.batchWriterSize", "100000")));
    bwc.setMaxWriteThreads(Integer.parseInt(
        getProperties().getProperty("accumulo.batchWriterThreads", "1")));

    bw = connector.createBatchWriter(table, bwc);

    // Create our scanners
    singleScanner = connector.createScanner(table, Authorizations.EMPTY);
    scanScanner = connector.createScanner(table, Authorizations.EMPTY);

    table = t; // Store the name of the table we have open.
  }

  /**
   * Gets a scanner from Accumulo over one row.
   * 
   * @param row
   *          the row to scan
   * @param fields
   *          the set of columns to scan
   * @return an Accumulo {@link Scanner} bound to the given row and columns
   */
  private Scanner getRow(Text row, Set<String> fields) {
    singleScanner.clearColumns();
    singleScanner.setRange(new Range(row));
    if (fields != null) {
      for (String field : fields) {
        singleScanner.fetchColumn(colFam, new Text(field));
      }
    }
    return singleScanner;
  }

  @Override
  public Status read(String t, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {

    try {
      checkTable(t);
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table." + e);
      return Status.ERROR;
    }

    try {
      // Pick out the results we care about.
      for (Entry<Key, Value> entry : getRow(new Text(key), null)) {
        Value v = entry.getValue();
        byte[] buf = v.get();
        result.put(entry.getKey().getColumnQualifier().toString(),
            new ByteArrayByteIterator(buf));
      }
    } catch (Exception e) {
      System.err.println("Error trying to reading Accumulo table" + key + e);
      return Status.ERROR;
    }
    return Status.OK;

  }

  @Override
  public Status scan(String t, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      checkTable(t);
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table." + e);
      return Status.ERROR;
    }

    // There doesn't appear to be a way to create a range for a given
    // LENGTH. Just start and end keys. So we'll do this the hard way for
    // now:
    // Just make the end 'infinity' and only read as much as we need.
    scanScanner.clearColumns();
    scanScanner.setRange(new Range(new Text(startkey), null));

    // Batch size is how many key/values to try to get per call. Here, I'm
    // guessing that the number of keys in a row is equal to the number of
    // fields
    // we're interested in.
    // We try to fetch one more so as to tell when we've run out of fields.

    if (fields != null) {
      // And add each of them as fields we want.
      for (String field : fields) {
        scanScanner.fetchColumn(colFam, new Text(field));
      }
    } // else - If no fields are provided, we assume one column/row.

    String rowKey = "";
    HashMap<String, ByteIterator> currentHM = null;
    int count = 0;

    // Begin the iteration.
    for (Entry<Key, Value> entry : scanScanner) {
      // Check for a new row.
      if (!rowKey.equals(entry.getKey().getRow().toString())) {
        if (count++ == recordcount) { // Done reading the last row.
          break;
        }
        rowKey = entry.getKey().getRow().toString();
        if (fields != null) {
          // Initial Capacity for all keys.
          currentHM = new HashMap<String, ByteIterator>(fields.size());
        } else {
          // An empty result map.
          currentHM = new HashMap<String, ByteIterator>();
        }
        result.add(currentHM);
      }
      // Now add the key to the hashmap.
      Value v = entry.getValue();
      byte[] buf = v.get();
      currentHM.put(entry.getKey().getColumnQualifier().toString(),
          new ByteArrayByteIterator(buf));
    }

    return Status.OK;
  }

  @Override
  public Status update(String t, String key,
      HashMap<String, ByteIterator> values) {
    try {
      checkTable(t);
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table." + e);
      return Status.ERROR;
    }

    Mutation mutInsert = new Mutation(new Text(key));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      mutInsert.put(colFam, new Text(entry.getKey()),
          System.currentTimeMillis(), new Value(entry.getValue().toArray()));
    }

    try {
      bw.addMutation(mutInsert);
      // Distributed YCSB co-ordination: YCSB on a client produces the key
      // to
      // be stored in the shared queue in ZooKeeper.
      if (pcFlag.equals(PC_PRODUCER)) {
        if (r.nextFloat() < 0.01) {
          keyNotification(key);
        }
      }
    } catch (MutationsRejectedException e) {
      System.err.println("Error performing update.");
      e.printStackTrace();
      return Status.ERROR;
    } catch (KeeperException e) {
      System.err.println("Error notifying the Zookeeper Queue.");
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String t, String key,
      HashMap<String, ByteIterator> values) {
    return update(t, key, values);
  }

  @Override
  public Status delete(String t, String key) {
    try {
      checkTable(t);
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table." + e);
      return Status.ERROR;
    }

    try {
      deleteRow(new Text(key));
    } catch (MutationsRejectedException e) {
      System.err.println("Error performing delete.");
      e.printStackTrace();
      return Status.ERROR;
    } catch (RuntimeException e) {
      System.err.println("Error performing delete.");
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  // These functions are adapted from RowOperations.java:
  private void deleteRow(Text row) throws MutationsRejectedException {
    deleteRow(getRow(row, null));
  }

  /**
   * Deletes a row, given a Scanner of JUST that row.
   */
  private void deleteRow(Scanner scanner) throws MutationsRejectedException {
    Mutation deleter = null;
    // iterate through the keys
    for (Entry<Key, Value> entry : scanner) {
      // create a mutation for the row
      if (deleter == null) {
        deleter = new Mutation(entry.getKey().getRow());
      }
      // the remove function adds the key with the delete flag set to true
      deleter.putDelete(entry.getKey().getColumnFamily(),
          entry.getKey().getColumnQualifier());
    }

    bw.addMutation(deleter);
  }

  private void keyNotification(String key) throws KeeperException {

    if (pcFlag.equals(PC_PRODUCER)) {
      try {
        q.produce(key);
      } catch (InterruptedException e) {
        // Reset the interrupted state.
        Thread.currentThread().interrupt();
      }
    } else {
      // XXX: do something better to keep the loop going (while??)
      for (int i = 0; i < 10000000; i++) {
        try {
          String strKey = q.consume();

          if (!hmKeyReads.containsKey(strKey)
              && !hmKeyNumReads.containsKey(strKey)) {
            hmKeyReads.put(strKey, new Long(System.currentTimeMillis()));
            hmKeyNumReads.put(strKey, new Integer(1));
          }

          // YCSB Consumer will read the key that was fetched from the
          // queue in ZooKeeper.
          // (current way is kind of ugly but works, i think)
          // TODO : Get table name from configuration or argument
          String usertable = "usertable";
          HashSet<String> fields = new HashSet<String>();
          for (int j = 0; j < 9; j++) {
            fields.add("field" + j);
          }
          HashMap<String, ByteIterator> result =
              new HashMap<String, ByteIterator>();

          read(usertable, strKey, fields, result);
          // If the results are empty, the key is enqueued in
          // Zookeeper
          // and tried again, until the results are found.
          if (result.size() == 0) {
            q.produce(strKey);
            int count = ((Integer) hmKeyNumReads.get(strKey)).intValue();
            hmKeyNumReads.put(strKey, new Integer(count + 1));
          } else {
            if (((Integer) hmKeyNumReads.get(strKey)).intValue() > 1) {
              long currTime = System.currentTimeMillis();
              long writeTime = ((Long) hmKeyReads.get(strKey)).longValue();
              System.out.println(
                  "Key=" + strKey + ";StartSearch=" + writeTime + ";EndSearch="
                      + currTime + ";TimeLag=" + (currTime - writeTime));
            }
          }
        } catch (InterruptedException e) {
          // Reset the interrupted state.
          Thread.currentThread().interrupt();
        }
      }
    }

  }

  public Status presplit(String t, String[] keys)
      throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    TreeSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < keys.length; i++) {
      splits.add(new Text(keys[i]));
    }
    connector.tableOperations().addSplits(t, splits);   
    return Status.OK;
  }

}
