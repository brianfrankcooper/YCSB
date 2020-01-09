/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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
package site.ycsb.db;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.hbase.async.Bytes;
import org.hbase.async.Config;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * Alternative Java client for Apache HBase.
 * 
 * This client provides a subset of the main HBase client and uses a completely
 * asynchronous pipeline for all calls. It is particularly useful for write heavy
 * workloads. It is also compatible with all production versions of HBase. 
 */
public class AsyncHBaseClient extends site.ycsb.DB {
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  private static final String CLIENT_SIDE_BUFFERING_PROPERTY = "clientbuffering";
  private static final String DURABILITY_PROPERTY = "durability";
  private static final String PREFETCH_META_PROPERTY = "prefetchmeta";
  private static final String CONFIG_PROPERTY = "config";
  private static final String COLUMN_FAMILY_PROPERTY = "columnfamily";
  private static final String JOIN_TIMEOUT_PROPERTY = "jointimeout";
  private static final String JOIN_TIMEOUT_PROPERTY_DEFAULT = "30000";
  
  /** Mutex for instantiating a single instance of the client. */
  private static final Object MUTEX = new Object();
  
  /** Use for tracking running thread counts so we know when to shutdown the client. */ 
  private static int threadCount = 0;
  
  /** The client that's used for all threads. */
  private static HBaseClient client;
  
  /** Print debug information to standard out. */
  private boolean debug = false;
  
  /** The column family use for the workload. */
  private byte[] columnFamilyBytes;
  
  /** Cache for the last table name/ID to avoid byte conversions. */
  private String lastTable = "";
  private byte[] lastTableBytes;
  
  private long joinTimeout;
  
  /** Whether or not to bypass the WAL for puts and deletes. */
  private boolean durability = true;
  
  /**
   * If true, buffer mutations on the client. This is the default behavior for
   * AsyncHBase. For measuring insert/update/delete latencies, client side
   * buffering should be disabled.
   * 
   * A single instance of this 
   */
  private boolean clientSideBuffering = false;
  
  @Override
  public void init() throws DBException {
    if (getProperties().getProperty(CLIENT_SIDE_BUFFERING_PROPERTY, "false")
        .toLowerCase().equals("true")) {
      clientSideBuffering = true;
    }
    if (getProperties().getProperty(DURABILITY_PROPERTY, "true")
        .toLowerCase().equals("false")) {
      durability = false;
    }
    final String columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROPERTY);
    if (columnFamily == null || columnFamily.isEmpty()) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = columnFamily.getBytes();
    
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
    
    joinTimeout = Integer.parseInt(getProperties().getProperty(
        JOIN_TIMEOUT_PROPERTY, JOIN_TIMEOUT_PROPERTY_DEFAULT));
    
    final boolean prefetchMeta = getProperties()
        .getProperty(PREFETCH_META_PROPERTY, "false")
        .toLowerCase().equals("true") ? true : false;
    try {
      synchronized (MUTEX) {
        ++threadCount;
        if (client == null) {
          final String configPath = getProperties().getProperty(CONFIG_PROPERTY);
          final Config config;
          if (configPath == null || configPath.isEmpty()) {
            config = new Config();
            final Iterator<Entry<Object, Object>> iterator = getProperties()
                 .entrySet().iterator();
            while (iterator.hasNext()) {
              final Entry<Object, Object> property = iterator.next();
              config.overrideConfig((String)property.getKey(), 
                  (String)property.getValue());
            }
          } else {
            config = new Config(configPath);
          }
          client = new HBaseClient(config);
          
          // Terminate right now if table does not exist, since the client
          // will not propagate this error upstream once the workload
          // starts.
          String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
          try {
            client.ensureTableExists(table).join(joinTimeout);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            throw new DBException(e);
          }
          
          if (prefetchMeta) {
            try {
              if (debug) {
                System.out.println("Starting meta prefetch for table " + table);
              }
              client.prefetchMeta(table).join(joinTimeout);
              if (debug) {
                System.out.println("Completed meta prefetch for table " + table);
              }
            } catch (InterruptedException e) {
              System.err.println("Interrupted during prefetch");
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              throw new DBException("Failed prefetch", e);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new DBException("Failed instantiation of client", e);
    }
  }
  
  @Override
  public void cleanup() throws DBException {
    synchronized (MUTEX) {
      --threadCount;
      if (client != null && threadCount < 1) {
        try {
          if (debug) {
            System.out.println("Shutting down client");
          }
          client.shutdown().joinUninterruptibly(joinTimeout);
        } catch (Exception e) {
          System.err.println("Failed to shutdown the AsyncHBase client "
              + "properly: " + e.getMessage());
        }
        client = null;
      }
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    setTable(table);
    
    final GetRequest get = new GetRequest(
        lastTableBytes, key.getBytes(), columnFamilyBytes);
    if (fields != null) {
      get.qualifiers(getQualifierList(fields));
    }
    
    try {
      if (debug) {
        System.out.println("Doing read from HBase columnfamily " + 
            Bytes.pretty(columnFamilyBytes));
        System.out.println("Doing read for key: " + key);
      }
      
      final ArrayList<KeyValue> row = client.get(get).join(joinTimeout);
      if (row == null || row.isEmpty()) {
        return Status.NOT_FOUND;
      }
      
      // got something so populate the results
      for (final KeyValue column : row) {
        result.put(new String(column.qualifier()), 
            // TODO - do we need to clone this array? YCSB may keep it in memory
            // for a while which would mean the entire KV would hang out and won't
            // be GC'd.
            new ByteArrayByteIterator(column.value()));
        
        if (debug) {
          System.out.println(
              "Result for field: " + Bytes.pretty(column.qualifier())
                  + " is: " + Bytes.pretty(column.value()));
        }
      }
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Thread interrupted");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      System.err.println("Failure reading from row with key " + key + 
          ": " + e.getMessage());
      return Status.ERROR;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    setTable(table);
    
    final Scanner scanner = client.newScanner(lastTableBytes);
    scanner.setFamily(columnFamilyBytes);
    scanner.setStartKey(startkey.getBytes(UTF8_CHARSET));
    // No end key... *sniff*
    if (fields != null) {
      scanner.setQualifiers(getQualifierList(fields));
    }
    
    // no filters? *sniff*
    ArrayList<ArrayList<KeyValue>> rows = null;
    try {
      int numResults = 0;
      while ((rows = scanner.nextRows().join(joinTimeout)) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final HashMap<String, ByteIterator> rowResult =
              new HashMap<String, ByteIterator>(row.size());
          for (final KeyValue column : row) {
            rowResult.put(new String(column.qualifier()), 
                // TODO - do we need to clone this array? YCSB may keep it in memory
                // for a while which would mean the entire KV would hang out and won't
                // be GC'd.
                new ByteArrayByteIterator(column.value()));
            if (debug) {
              System.out.println("Got scan result for key: " + 
                  Bytes.pretty(column.key()));
            }
          }
          result.add(rowResult);
          numResults++;

          if (numResults >= recordcount) {// if hit recordcount, bail out
            break;
          }
        }
      }
      scanner.close().join(joinTimeout);
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Thread interrupted");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      System.err.println("Failure reading from row with key " + startkey + 
          ": " + e.getMessage());
      return Status.ERROR;
    }
    
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    setTable(table);
    
    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    
    final byte[][] qualifiers = new byte[values.size()][];
    final byte[][] byteValues = new byte[values.size()][];
    
    int idx = 0;
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      qualifiers[idx] = entry.getKey().getBytes();
      byteValues[idx++] = entry.getValue().toArray();
      if (debug) {
        System.out.println("Adding field/value " + entry.getKey() + "/"
            + Bytes.pretty(entry.getValue().toArray()) + " to put request");
      }
    }
    
    final PutRequest put = new PutRequest(lastTableBytes, key.getBytes(), 
        columnFamilyBytes, qualifiers, byteValues);
    if (!durability) {
      put.setDurable(false);
    }
    if (!clientSideBuffering) {
      put.setBufferable(false);
      try {
        client.put(put).join(joinTimeout);
      } catch (InterruptedException e) {
        System.err.println("Thread interrupted");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.err.println("Failure reading from row with key " + key + 
            ": " + e.getMessage());
        return Status.ERROR;
      }
    } else {
      // hooray! Asynchronous write. But without a callback and an async
      // YCSB call we don't know whether it succeeded or not
      client.put(put);
    }
    
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    setTable(table);
    
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }
    
    final DeleteRequest delete = new DeleteRequest(
        lastTableBytes, key.getBytes(), columnFamilyBytes);
    if (!durability) {
      delete.setDurable(false);
    }
    if (!clientSideBuffering) {
      delete.setBufferable(false);
      try {
        client.delete(delete).join(joinTimeout);
      } catch (InterruptedException e) {
        System.err.println("Thread interrupted");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.err.println("Failure reading from row with key " + key + 
            ": " + e.getMessage());
        return Status.ERROR;
      }
    } else {
      // hooray! Asynchronous write. But without a callback and an async
      // YCSB call we don't know whether it succeeded or not
      client.delete(delete);
    }
    return Status.OK;
  }

  /**
   * Little helper to set the table byte array. If it's different than the last
   * table we reset the byte array. Otherwise we just use the existing array.
   * @param table The table we're operating against
   */
  private void setTable(final String table) {
    if (!lastTable.equals(table)) {
      lastTable = table;
      lastTableBytes = table.getBytes();
    }
  }
  
  /**
   * Little helper to build a qualifier byte array from a field set.
   * @param fields The fields to fetch.
   * @return The column qualifier byte arrays.
   */
  private byte[][] getQualifierList(final Set<String> fields) {
    final byte[][] qualifiers = new byte[fields.size()][];
    int idx = 0;
    for (final String field : fields) {
      qualifiers[idx++] = field.getBytes();
    }
    return qualifiers;
  }
}