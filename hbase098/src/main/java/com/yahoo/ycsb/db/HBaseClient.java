/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * HBase client for YCSB framework.
 */
public class HBaseClient extends com.yahoo.ycsb.DB {
  private static final Configuration CONFIG = HBaseConfiguration.create();
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private boolean debug = false;

  private String tableName = "";
  private static HConnection hConn = null;
  private HTableInterface hTable = null;
  private String columnFamily = "";
  private byte[] columnFamilyBytes;
  private boolean clientSideBuffering = false;
  private long writeBufferSize = 1024 * 1024 * 12;
  /**
   * Whether or not a page filter should be used to limit scan length.
   */
  private boolean usePageFilter = true;

  private static final Object TABLE_LOCK = new Object();

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null) &&
        (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }

    if (getProperties().containsKey("clientbuffering")) {
      clientSideBuffering = Boolean.parseBoolean(getProperties().getProperty("clientbuffering"));
    }
    if (getProperties().containsKey("writebuffersize")) {
      writeBufferSize = Long.parseLong(getProperties().getProperty("writebuffersize"));
    }
    if ("false".equals(getProperties().getProperty("hbase.usepagefilter", "true"))) {
      usePageFilter = false;
    }
    if ("kerberos".equalsIgnoreCase(CONFIG.get("hbase.security.authentication"))) {
      CONFIG.set("hadoop.security.authentication", "Kerberos");
      UserGroupInformation.setConfiguration(CONFIG);
    }
    if ((getProperties().getProperty("principal") != null) && (getProperties().getProperty("keytab") != null)) {
      try {
        UserGroupInformation.loginUserFromKeytab(getProperties().getProperty("principal"),
            getProperties().getProperty("keytab"));
      } catch (IOException e) {
        System.err.println("Keytab file is not readable or not found");
        throw new DBException(e);
      }
    }
    try {
      THREAD_COUNT.getAndIncrement();
      synchronized (THREAD_COUNT) {
        if (hConn == null) {
          hConn = HConnectionManager.createConnection(CONFIG);
        }
      }
    } catch (IOException e) {
      System.err.println("Connection to HBase was not successful");
      throw new DBException(e);
    }
    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase tableName");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);

    // Terminate right now if tableName does not exist, since the client
    // will not propagate this error upstream once the workload
    // starts.
    String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    try {
      HTableInterface ht = hConn.getTable(table);
      ht.getTableDescriptor();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    // Get the measurements instance as this is the only client that should
    // count clean up time like an update since autoflush is off.
    Measurements measurements = Measurements.getMeasurements();
    try {
      long st = System.nanoTime();
      if (hTable != null) {
        hTable.flushCommits();
      }
      synchronized (THREAD_COUNT) {
        int threadCount = THREAD_COUNT.decrementAndGet();
        if (threadCount <= 0 && hConn != null) {
          hConn.close();
        }
      }
      long en = System.nanoTime();
      measurements.measure("UPDATE", (int) ((en - st) / 1000));
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  private void getHTable(String table) throws IOException {
    synchronized (TABLE_LOCK) {
      hTable = hConn.getTable(table);
      //2 suggestions from http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
      hTable.setAutoFlush(!clientSideBuffering, true);
      hTable.setWriteBufferSize(writeBufferSize);
      //return hTable;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the tableName
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    //if this is a "new" tableName, init HTable object.  Else, use existing one
    if (!this.tableName.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase tableName: " + e);
        return Status.ERROR;
      }
    }

    Result r;
    try {
      if (debug) {
        System.out.println("Doing read from HBase columnfamily " + columnFamily);
        System.out.println("Doing read for key: " + key);
      }
      Get g = new Get(Bytes.toBytes(key));
      if (fields == null) {
        g.addFamily(columnFamilyBytes);
      } else {
        for (String field : fields) {
          g.addColumn(columnFamilyBytes, Bytes.toBytes(field));
        }
      }
      r = hTable.get(g);
    } catch (IOException e) {
      System.err.println("Error doing get: " + e);
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      //do nothing for now...need to understand HBase concurrency model better
      return Status.ERROR;
    }

    for (KeyValue kv : r.raw()) {
      result.put(
          Bytes.toString(kv.getQualifier()),
          new ByteArrayByteIterator(kv.getValue()));
      if (debug) {
        System.out.println("Result for field: " + Bytes.toString(kv.getQualifier()) +
            " is: " + Bytes.toString(kv.getValue()));
      }

    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table       The name of the tableName
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    //if this is a "new" tableName, init HTable object.  Else, use existing one
    if (!this.tableName.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase tableName: " + e);
        return Status.ERROR;
      }
    }

    Scan s = new Scan(Bytes.toBytes(startkey));
    //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
    //We get back recordcount records
    s.setCaching(recordcount);
    if (this.usePageFilter) {
      s.setFilter(new PageFilter(recordcount));
    }

    //add specified fields or else all fields
    if (fields == null) {
      s.addFamily(columnFamilyBytes);
    } else {
      for (String field : fields) {
        s.addColumn(columnFamilyBytes, Bytes.toBytes(field));
      }
    }

    //get results
    try (ResultScanner scanner = hTable.getScanner(s)) {
      int numResults = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        //get row key
        String key = Bytes.toString(rr.getRow());
        if (debug) {
          System.out.println("Got scan result for key: " + key);
        }

        HashMap<String, ByteIterator> rowResult = new HashMap<>();

        for (KeyValue kv : rr.raw()) {
          rowResult.put(
              Bytes.toString(kv.getQualifier()),
              new ByteArrayByteIterator(kv.getValue()));
        }
        //add rowResult to result vector
        result.add(rowResult);
        numResults++;

        // PageFilter does not guarantee that the number of results is <= pageSize, so this
        // break is required.
        //if hit recordcount, bail out
        if (numResults >= recordcount) {
          break;
        }
      } //done with row

    } catch (IOException e) {
      if (debug) {
        System.out.println("Error in getting/parsing scan result: " + e);
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the tableName
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    //if this is a "new" tableName, init HTable object.  Else, use existing one
    if (!this.tableName.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase tableName: " + e);
        return Status.ERROR;
      }
    }


    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    Put p = new Put(Bytes.toBytes(key));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] value = entry.getValue().toArray();
      if (debug) {
        System.out.println("Adding field/value " + entry.getKey() + "/" +
            Bytes.toStringBinary(value) + " to put request");
      }
      p.add(columnFamilyBytes, Bytes.toBytes(entry.getKey()), value);
    }

    try {
      hTable.put(p);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing put: " + e);
      }
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      //do nothing for now...hope this is rare
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table  The name of the tableName
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the tableName
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public Status delete(String table, String key) {
    //if this is a "new" tableName, init HTable object.  Else, use existing one
    if (!this.tableName.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase tableName: " + e);
        return Status.ERROR;
      }
    }

    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    Delete d = new Delete(Bytes.toBytes(key));
    try {
      hTable.delete(d);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing delete: " + e);
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Please specify a threadcount, columnfamily and operation count");
      System.exit(0);
    }

    final int keyspace = 10000; //120000000;

    final int threadcount = Integer.parseInt(args[0]);

    final String columnfamily = args[1];


    final int opcount = Integer.parseInt(args[2]) / threadcount;

    Vector<Thread> allthreads = new Vector<>();

    for (int i = 0; i < threadcount; i++) {
      Thread t = new Thread() {
        public void run() {
          try {
            Random random = new Random();

            HBaseClient cli = new HBaseClient();

            Properties props = new Properties();
            props.setProperty("columnfamily", columnfamily);
            props.setProperty("debug", "true");
            cli.setProperties(props);

            cli.init();

            long accum = 0;

            for (int i = 0; i < opcount; i++) {
              int keynum = random.nextInt(keyspace);
              String key = "user" + keynum;
              long st = System.currentTimeMillis();
              Status result;
              Vector<HashMap<String, ByteIterator>> scanResults = new Vector<>();
              Set<String> scanFields = new HashSet<String>();
              result = cli.scan("table1", "user2", 20, null, scanResults);

              long en = System.currentTimeMillis();

              accum += (en - st);

              if (!result.equals(Status.OK)) {
                System.out.println("Error " + result + " for " + key);
              }

              if (i % 10 == 0) {
                System.out.println(i + " operations, average latency: " + (((double) accum) / ((double) i)));
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      allthreads.add(t);
    }

    long st = System.currentTimeMillis();
    for (Thread t : allthreads) {
      t.start();
    }

    for (Thread t : allthreads) {
      try {
        t.join();
      } catch (InterruptedException ignored) {
        System.err.println("interrupted");
        Thread.currentThread().interrupt();
      }
    }
    long en = System.currentTimeMillis();

    System.out.println("Throughput: " + ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st))))
        + " ops/sec");
  }
}
