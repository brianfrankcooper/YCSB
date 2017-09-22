/**
 * Copyright (c) 2011 YCSB++ project, 2014-2016 YCSB contributors.
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

package com.yahoo.ycsb.db.accumulo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
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
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CleanUp;
import org.apache.hadoop.io.Text;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * <a href="https://accumulo.apache.org/">Accumulo</a> binding for YCSB.
 */
public class AccumuloClient extends DB {

  private ZooKeeperInstance inst;
  private Connector connector;
  private Text colFam = new Text("");
  private byte[] colFamBytes = new byte[0];
  private final ConcurrentHashMap<String, BatchWriter> writers = new ConcurrentHashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        CleanUp.shutdownNow();
      }
    });
  }

  @Override
  public void init() throws DBException {
    colFam = new Text(getProperties().getProperty("accumulo.columnFamily"));
    colFamBytes = colFam.toString().getBytes(UTF_8);

    inst = new ZooKeeperInstance(new ClientConfiguration()
        .withInstance(getProperties().getProperty("accumulo.instanceName"))
        .withZkHosts(getProperties().getProperty("accumulo.zooKeepers")));
    try {
      String principal = getProperties().getProperty("accumulo.username");
      AuthenticationToken token =
          new PasswordToken(getProperties().getProperty("accumulo.password"));
      connector = inst.getConnector(principal, token);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new DBException(e);
    }

    if (!(getProperties().getProperty("accumulo.pcFlag", "none").equals("none"))) {
      System.err.println("Sorry, the ZK based producer/consumer implementation has been removed. " +
          "Please see YCSB issue #416 for work on adding a general solution to coordinated work.");
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      Iterator<BatchWriter> iterator = writers.values().iterator();
      while (iterator.hasNext()) {
        BatchWriter writer = iterator.next();
        writer.close();
        iterator.remove();
      }
    } catch (MutationsRejectedException e) {
      throw new DBException(e);
    }
  }

  /**
   * Called when the user specifies a table that isn't the same as the existing
   * table. Connect to it and if necessary, close our current connection.
   * 
   * @param table
   *          The table to open.
   */
  public BatchWriter getWriter(String table) throws TableNotFoundException {
    // tl;dr We're paying a cost for the ConcurrentHashMap here to deal with the DB api.
    // We know that YCSB is really only ever going to send us data for one table, so using
    // a concurrent data structure is overkill (especially in such a hot code path).
    // However, the impact seems to be relatively negligible in trivial local tests and it's
    // "more correct" WRT to the API.
    BatchWriter writer = writers.get(table);
    if (null == writer) {
      BatchWriter newWriter = createBatchWriter(table);
      BatchWriter oldWriter = writers.putIfAbsent(table, newWriter);
      // Someone beat us to creating a BatchWriter for this table, use their BatchWriters
      if (null != oldWriter) {
        try {
          // Make sure to clean up our new batchwriter!
          newWriter.close();
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
        writer = oldWriter;
      } else {
        writer = newWriter;
      }
    }
    return writer;
  }

  /**
   * Creates a BatchWriter with the expected configuration.
   *
   * @param table The table to write to
   */
  private BatchWriter createBatchWriter(String table) throws TableNotFoundException {
    BatchWriterConfig bwc = new BatchWriterConfig();
    bwc.setMaxLatency(
        Long.parseLong(getProperties()
            .getProperty("accumulo.batchWriterMaxLatency", "30000")),
        TimeUnit.MILLISECONDS);
    bwc.setMaxMemory(Long.parseLong(
        getProperties().getProperty("accumulo.batchWriterSize", "100000")));
    final String numThreadsValue = getProperties().getProperty("accumulo.batchWriterThreads");
    // Try to saturate the client machine.
    int numThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    if (null != numThreadsValue) {
      numThreads = Integer.parseInt(numThreadsValue);
    }
    System.err.println("Using " + numThreads + " threads to write data");
    bwc.setMaxWriteThreads(numThreads);
    return connector.createBatchWriter(table, bwc);
  }

  /**
   * Gets a scanner from Accumulo over one row.
   *
   * @param row the row to scan
   * @param fields the set of columns to scan
   * @return an Accumulo {@link Scanner} bound to the given row and columns
   */
  private Scanner getRow(String table, Text row, Set<String> fields) throws TableNotFoundException {
    Scanner scanner = connector.createScanner(table, Authorizations.EMPTY);
    scanner.setRange(new Range(row));
    if (fields != null) {
      for (String field : fields) {
        scanner.fetchColumn(colFam, new Text(field));
      }
    }
    return scanner;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {

    Scanner scanner = null;
    try {
      scanner = getRow(table, new Text(key), null);
      // Pick out the results we care about.
      final Text cq = new Text();
      for (Entry<Key, Value> entry : scanner) {
        entry.getKey().getColumnQualifier(cq);
        Value v = entry.getValue();
        byte[] buf = v.get();
        result.put(cq.toString(),
            new ByteArrayByteIterator(buf));
      }
    } catch (Exception e) {
      System.err.println("Error trying to reading Accumulo table " + table + " " + key);
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      if (null != scanner) {
        scanner.close();
      }
    }
    return Status.OK;

  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // Just make the end 'infinity' and only read as much as we need.
    Scanner scanner = null;
    try {
      scanner = connector.createScanner(table, Authorizations.EMPTY);
      scanner.setRange(new Range(new Text(startkey), null));

      // Have Accumulo send us complete rows, serialized in a single Key-Value pair
      IteratorSetting cfg = new IteratorSetting(100, WholeRowIterator.class);
      scanner.addScanIterator(cfg);

      // If no fields are provided, we assume one column/row.
      if (fields != null) {
        // And add each of them as fields we want.
        for (String field : fields) {
          scanner.fetchColumn(colFam, new Text(field));
        }
      }

      int count = 0;
      for (Entry<Key, Value> entry : scanner) {
        // Deserialize the row
        SortedMap<Key, Value> row = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
        HashMap<String, ByteIterator> rowData;
        if (null != fields) {
          rowData = new HashMap<>(fields.size());
        } else {
          rowData = new HashMap<>();
        }
        result.add(rowData);
        // Parse the data in the row, avoid unnecessary Text object creation
        final Text cq = new Text();
        for (Entry<Key, Value> rowEntry : row.entrySet()) {
          rowEntry.getKey().getColumnQualifier(cq);
          rowData.put(cq.toString(), new ByteArrayByteIterator(rowEntry.getValue().get()));
        }
        if (count++ == recordcount) { // Done reading the last row.
          break;
        }
      }
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table.");
      e.printStackTrace();
      return Status.ERROR;
    } catch (IOException e) {
      System.err.println("Error deserializing data from Accumulo.");
      e.printStackTrace();
      return Status.ERROR;
    } finally {
      if (null != scanner) {
        scanner.close();
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    BatchWriter bw = null;
    try {
      bw = getWriter(table);
    } catch (TableNotFoundException e) {
      System.err.println("Error opening batch writer to Accumulo table " + table);
      e.printStackTrace();
      return Status.ERROR;
    }

    Mutation mutInsert = new Mutation(key.getBytes(UTF_8));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      mutInsert.put(colFamBytes, entry.getKey().getBytes(UTF_8), entry.getValue().toArray());
    }

    try {
      bw.addMutation(mutInsert);
    } catch (MutationsRejectedException e) {
      System.err.println("Error performing update.");
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.BATCHED_OK;
  }

  @Override
  public Status insert(String t, String key,
                       Map<String, ByteIterator> values) {
    return update(t, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    BatchWriter bw;
    try {
      bw = getWriter(table);
    } catch (TableNotFoundException e) {
      System.err.println("Error trying to connect to Accumulo table.");
      e.printStackTrace();
      return Status.ERROR;
    }

    try {
      deleteRow(table, new Text(key), bw);
    } catch (TableNotFoundException | MutationsRejectedException e) {
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
  private void deleteRow(String table, Text row, BatchWriter bw) throws MutationsRejectedException,
          TableNotFoundException {
    // TODO Use a batchDeleter instead
    deleteRow(getRow(table, row, null), bw);
  }

  /**
   * Deletes a row, given a Scanner of JUST that row.
   */
  private void deleteRow(Scanner scanner, BatchWriter bw) throws MutationsRejectedException {
    Mutation deleter = null;
    // iterate through the keys
    final Text row = new Text();
    final Text cf = new Text();
    final Text cq = new Text();
    for (Entry<Key, Value> entry : scanner) {
      // create a mutation for the row
      if (deleter == null) {
        entry.getKey().getRow(row);
        deleter = new Mutation(row);
      }
      entry.getKey().getColumnFamily(cf);
      entry.getKey().getColumnQualifier(cq);
      // the remove function adds the key with the delete flag set to true
      deleter.putDelete(cf, cq);
    }

    bw.addMutation(deleter);
  }
}
