/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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

package com.yahoo.ycsb;

import java.util.*;
import java.util.Map.Entry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class BasicDB extends DB {
  public static final String COUNT = "basicdb.count";
  public static final String COUNT_DEFAULT = "false";
  
  public static final String VERBOSE = "basicdb.verbose";
  public static final String VERBOSE_DEFAULT = "true";

  public static final String SIMULATE_DELAY = "basicdb.simulatedelay";
  public static final String SIMULATE_DELAY_DEFAULT = "0";

  public static final String RANDOMIZE_DELAY = "basicdb.randomizedelay";
  public static final String RANDOMIZE_DELAY_DEFAULT = "true";

  protected static final Object MUTEX = new Object();
  protected static int counter = 0;
  protected static Map<Integer, Integer> reads;
  protected static Map<Integer, Integer> scans;
  protected static Map<Integer, Integer> updates;
  protected static Map<Integer, Integer> inserts;
  protected static Map<Integer, Integer> deletes;
  
  protected boolean verbose;
  protected boolean randomizedelay;
  protected int todelay;
  protected boolean count;

  public BasicDB() {
    todelay = 0;
  }

  protected void delay() {
    if (todelay > 0) {
      long delayNs;
      if (randomizedelay) {
        delayNs = TimeUnit.MILLISECONDS.toNanos(Utils.random().nextInt(todelay));
        if (delayNs == 0) {
          return;
        }
      } else {
        delayNs = TimeUnit.MILLISECONDS.toNanos(todelay);
      }

      final long deadline = System.nanoTime() + delayNs;
      do {
        LockSupport.parkNanos(deadline - System.nanoTime());
      } while (System.nanoTime() < deadline && !Thread.interrupted());
    }
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() {
    verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
    todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
    randomizedelay = Boolean.parseBoolean(getProperties().getProperty(RANDOMIZE_DELAY, RANDOMIZE_DELAY_DEFAULT));
    count = Boolean.parseBoolean(getProperties().getProperty(COUNT, COUNT_DEFAULT));
    if (verbose) {
      synchronized (System.out) {
        System.out.println("***************** properties *****************");
        Properties p = getProperties();
        if (p != null) {
          for (Enumeration e = p.propertyNames(); e.hasMoreElements();) {
            String k = (String) e.nextElement();
            System.out.println("\"" + k + "\"=\"" + p.getProperty(k) + "\"");
          }
        }
        System.out.println("**********************************************");
      }
    }
    
    synchronized (MUTEX) {
      if (counter == 0 && count) {
        reads = new HashMap<Integer, Integer>();
        scans = new HashMap<Integer, Integer>();
        updates = new HashMap<Integer, Integer>();
        inserts = new HashMap<Integer, Integer>();
        deletes = new HashMap<Integer, Integer>();
      }
      counter++;
    }
  }

  protected static final ThreadLocal<StringBuilder> TL_STRING_BUILDER = new ThreadLocal<StringBuilder>() {
    @Override
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
  };

  protected static StringBuilder getStringBuilder() {
    StringBuilder sb = TL_STRING_BUILDER.get();
    sb.setLength(0);
    return sb;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("READ ").append(table).append(" ").append(key).append(" [ ");
      if (fields != null) {
        for (String f : fields) {
          sb.append(f).append(" ");
        }
      } else {
        sb.append("<all fields>");
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(reads, hash(table, key, fields));
    }
    
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("SCAN ").append(table).append(" ").append(startkey).append(" ").append(recordcount).append(" [ ");
      if (fields != null) {
        for (String f : fields) {
          sb.append(f).append(" ");
        }
      } else {
        sb.append("<all fields>");
      }

      sb.append("]");
      System.out.println(sb);
    }
    
    if (count) {
      incCounter(scans, hash(table, startkey, fields));
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(updates, hash(table, key, values));
    }
    
    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(inserts, hash(table, key, values));
    }
    
    return Status.OK;
  }


  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public Status delete(String table, String key) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("DELETE ").append(table).append(" ").append(key);
      System.out.println(sb);
    }

    if (count) {
      incCounter(deletes, (table + key).hashCode());
    }
    
    return Status.OK;
  }

  @Override
  public void cleanup() {
    synchronized (MUTEX) {
      int countDown = --counter;
      if (count && countDown < 1) {
        // TODO - would be nice to call something like: 
        // Measurements.getMeasurements().oneOffMeasurement("READS", "Uniques", reads.size());
        System.out.println("[READS], Uniques, " + reads.size());
        System.out.println("[SCANS], Uniques, " + scans.size());
        System.out.println("[UPDATES], Uniques, " + updates.size());
        System.out.println("[INSERTS], Uniques, " + inserts.size());
        System.out.println("[DELETES], Uniques, " + deletes.size());
      }
    }
  }
  
  /**
   * Increments the count on the hash in the map.
   * @param map A non-null map to sync and use for incrementing.
   * @param hash A hash code to increment.
   */
  protected void incCounter(final Map<Integer, Integer> map, final int hash) {
    synchronized (map) {
      Integer ctr = map.get(hash);
      if (ctr == null) {
        map.put(hash, 1);
      } else {
        map.put(hash, ctr + 1);
      }
    }
  }
  
  /**
   * Hashes the table, key and fields, sorting the fields first for a consistent
   * hash.
   * Note that this is expensive as we generate a copy of the fields and a string
   * buffer to hash on. Hashing on the objects is problematic.
   * @param table The user table.
   * @param key The key read or scanned.
   * @param fields The fields read or scanned.
   * @return The hash code.
   */
  protected int hash(final String table, final String key, final Set<String> fields) {
    if (fields == null) {
      return (table + key).hashCode();
    }
    StringBuilder buf = getStringBuilder().append(table).append(key);
    List<String> sorted = new ArrayList<String>(fields);
    Collections.sort(sorted);
    for (final String field : sorted) {
      buf.append(field);
    }
    return buf.toString().hashCode();
  }
  
  /**
   * Hashes the table, key and fields, sorting the fields first for a consistent
   * hash.
   * Note that this is expensive as we generate a copy of the fields and a string
   * buffer to hash on. Hashing on the objects is problematic.
   * @param table The user table.
   * @param key The key read or scanned.
   * @param values The values to hash on.
   * @return The hash code.
   */
  protected int hash(final String table, final String key, final Map<String, ByteIterator> values) {
    if (values == null) {
      return (table + key).hashCode();
    }
    final TreeMap<String, ByteIterator> sorted = 
        new TreeMap<String, ByteIterator>(values);
    
    StringBuilder buf = getStringBuilder().append(table).append(key);
    for (final Entry<String, ByteIterator> entry : sorted.entrySet()) {
      entry.getValue().reset();
      buf.append(entry.getKey())
         .append(entry.getValue().toString());
    }
    return buf.toString().hashCode();
  }
  
  /**
   * Short test of BasicDB
   */
  /*
  public static void main(String[] args) {
    BasicDB bdb = new BasicDB();

    Properties p = new Properties();
    p.setProperty("Sky", "Blue");
    p.setProperty("Ocean", "Wet");

    bdb.setProperties(p);

    bdb.init();

    HashMap<String, String> fields = new HashMap<String, String>();
    fields.put("A", "X");
    fields.put("B", "Y");

    bdb.read("table", "key", null, null);
    bdb.insert("table", "key", fields);

    fields = new HashMap<String, String>();
    fields.put("C", "Z");

    bdb.update("table", "key", fields);

    bdb.delete("table", "key");
  }
  */
}
