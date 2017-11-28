/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ignite client.
 * <p>
 * See {@code ignite/README.md} for details.
 *
 * @author spuchnin
 */
public class IgniteClient extends DB {
  private static final String DEFAULT_CACHE_NAME = "usertable";
  private static final String HOSTS_PROPERTY = "hosts";
  private static final String PORTS_PROPERTY = "ports";
  private static final String CLIENT_NODE_NAME = "YCSB client node";
  private static final String PORTS_DEFAULTS = "47500..47509";
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  /** Ignite cluster. */
  private static Ignite cluster = null;
  /** Ignite cache to store key-values. */
  private static IgniteCache<String, BinaryObject> cache = null;
  /** Debug flag. */
  private static boolean debug = false;

  /** Cached binary type. */
  private BinaryType binType = null;
  /** Cached binary type's fields. */
  private final ConcurrentHashMap<String, BinaryField> fieldsCache = new ConcurrentHashMap<>();

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }

      try {
        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        IgniteConfiguration igcfg = new IgniteConfiguration();
        igcfg.setIgniteInstanceName(CLIENT_NODE_NAME);

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
                    "Required property \"%s\" missing for Ignite Cluster",
                    HOSTS_PROPERTY));
        }

        String ports = getProperties().getProperty(PORTS_PROPERTY, PORTS_DEFAULTS);

        if (ports == null) {
          throw new DBException(String.format(
                    "Required property \"%s\" missing for Ignite Cluster",
                    PORTS_PROPERTY));

        }

        System.setProperty("IGNITE_QUIET", "false");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        Collection<String> addrs = new LinkedHashSet<>();
        addrs.add(host + ":" + ports);

        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

        ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(addrs);
        disco.setIpFinder(ipFinder);

        igcfg.setDiscoverySpi(disco);
        igcfg.setNetworkTimeout(2000);
        igcfg.setClientMode(true);

        System.out.println("Before cluster start");
        cluster = Ignition.start(igcfg);
        System.out.println("Before cluster activate");
        cluster.active(true);

        cache = cluster.cache(DEFAULT_CACHE_NAME).withKeepBinary();
      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        cluster.close();
        cluster = null;
      }

      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
                  String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      BinaryObject po = cache.get(key);

      if (po == null) {
        return Status.NOT_FOUND;
      }

      if (binType == null) {
        binType = po.type();
      }

      for (String s : F.isEmpty(fields) ? binType.fieldNames() : fields) {
        BinaryField bfld = fieldsCache.get(s);

        if (bfld == null) {
          bfld = binType.field(s);
          fieldsCache.put(s, bfld);
        }

        String val = bfld.value(po);
        if (val != null) {
          result.put(s, new StringByteIterator(val));
        }

        if (debug) {
          System.out.println("table:{" + table + "}, key:{" + key + "}" + ", fields:{" + fields + "}");
          System.out.println("fields in po{" + binType.fieldNames() + "}");
          System.out.println("result {" + result + "}");
        }
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * <p>
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("Scan method isn't implemented");
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      cache.invoke(key, new Updater(values));

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.out.println("Error updating key: " + key);
      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      BinaryObjectBuilder bob = cluster.binary().builder("CustomType");

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        bob.setField(entry.getKey(), entry.getValue().toString());

        if (debug) {
          System.out.println(entry.getKey() + ":" + entry.getValue());
        }
      }

      BinaryObject bo = bob.build();

      if (table.equals(DEFAULT_CACHE_NAME)) {
        cache.put(key, bo);
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.out.println("Error inserting key: " + key);
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    try {
      cache.remove(key);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error deleting key: " + key);
    }

    return Status.ERROR;
  }

  /**
   * Entry processor to update values.
   */
  public static class Updater implements CacheEntryProcessor<String, BinaryObject, Object> {
    private String[] flds;
    private String[] vals;

    /**
     * @param values Updated fields.
     */
    Updater(Map<String, ByteIterator> values) {
      flds = new String[values.size()];
      vals = new String[values.size()];

      int idx = 0;
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        flds[idx] = e.getKey();
        vals[idx] = e.getValue().toString();
        ++idx;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object process(MutableEntry<String, BinaryObject> mutableEntry, Object... objects)
              throws EntryProcessorException {
      BinaryObjectBuilder bob = mutableEntry.getValue().toBuilder();

      for (int i = 0; i < flds.length; ++i) {
        bob.setField(flds[i], vals[i]);
      }

      mutableEntry.setValue(bob.build());

      return null;
    }
  }
}
