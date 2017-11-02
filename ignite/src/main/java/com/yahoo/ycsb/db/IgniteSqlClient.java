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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ignite client.
 * <p>
 * See {@code ignite/README.md} for details.
 *
 * @author spuchnin
 */
public class IgniteSqlClient extends DB {
  private static final String DEFAULT_CACHE_NAME = "usertable";
  private static final String HOSTS_PROPERTY = "hosts";
  private static final String PORTS_PROPERTY = "ports";
  private static final String CLIENT_NODE_NAME = "YCSB client node";
  private static final String PORTS_DEFAULTS = "47500..47509";
  private static final String PRIMARY_KEY = "YCSB_KEY";
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static Ignite cluster = null;
  private static IgniteCache<String, BinaryObject> cache = null;
  private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
  private static boolean debug = false;

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

        ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(addrs);
        disco.setIpFinder(ipFinder);

        igcfg.setDiscoverySpi(disco);
        igcfg.setNetworkTimeout(2000);
        igcfg.setClientMode(true);

        CacheConfiguration<String, BinaryObject> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName(DEFAULT_CACHE_NAME);

        System.out.println("Start Ignite client node.");
        cluster = Ignition.start(igcfg);

        System.out.println("Activate Ignite cluster.");
        cluster.active(true);

        cache = cluster.getOrCreateCache(cacheCfg).withKeepBinary();

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
      StringBuilder sb = new StringBuilder("SELECT * FROM ").append(table)
                .append(" WHERE ").append(PRIMARY_KEY).append("=?");

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(key);

      FieldsQueryCursor<List<?>> cur = cache.query(qry);
      Iterator<List<?>> it = cur.iterator();

      if (!it.hasNext()) {
        return Status.NOT_FOUND;
      }

      String[] colNames = new String[cur.getColumnsCount()];
      for (int i = 0; i < colNames.length; ++i) {
        String colName = cur.getFieldName(i);
        if (F.isEmpty(fields)) {
          colNames[i] = colName.toLowerCase();
        } else {
          for (String f : fields) {
            if (f.equalsIgnoreCase(colName)) {
              colNames[i] = f;
            }
          }
        }
      }

      while (it.hasNext()) {
        List<?> row = it.next();

        for (int i = 0; i < colNames.length; ++i) {
          if (colNames[i] != null) {
            result.put(colNames[i], new StringByteIterator((String) row.get(i)));
          }
        }
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error in processing read from table: " + table);
      e.printStackTrace(System.err);
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
    try {
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error scanning with startkey: " + startkey);
      return Status.ERROR;
    }

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
      UpdateData updData = new UpdateData(key, values);
      StringBuilder sb = new StringBuilder("UPDATE ").append(table).append(" SET ");

      for (int i = 0; i < updData.getFields().length; ++i) {
        sb.append(updData.getFields()[i]).append("=?");
        if (i < updData.getFields().length - 1) {
          sb.append(", ");
        }
      }

      sb.append(" WHERE ").append(PRIMARY_KEY).append("=?");

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(updData.getArgs());

      cache.query(qry).getAll();

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error in processing update table: " + table);
      e.printStackTrace(System.err);
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
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      InsertData insertData = new InsertData(key, values);
      StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (")
                .append(insertData.getInsertFields()).append(") VALUES (")
                .append(insertData.getInsertParams()).append(')');

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(insertData.getArgs());

      cache.query(qry).getAll();

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error in processing insert to table: " + table);
      e.printStackTrace(System.err);
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
      StringBuilder sb = new StringBuilder("DELETE FROM ").append(table)
                .append(" WHERE ").append(PRIMARY_KEY).append(" = ?");

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(key);
      cache.query(qry).getAll();
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error in processing read from table: " + table);
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }

  /**
   * Field and values for insert queries.
   */
  private static class InsertData {
    private final Object[] args;
    private final String insertFields;
    private final String insertParams;

    /**
     * @param key    Key.
     * @param values Field values.
     */
    InsertData(String key, Map<String, ByteIterator> values) {
      args = new String[values.size() + 1];

      int idx = 0;
      args[idx++] = key;

      StringBuilder sbFields = new StringBuilder(PRIMARY_KEY);
      StringBuilder sbParams = new StringBuilder("?");

      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        args[idx++] = e.getValue().toString();
        sbFields.append(',').append(e.getKey());
        sbParams.append(", ?");
      }

      insertFields = sbFields.toString();
      insertParams = sbParams.toString();
    }

    public Object[] getArgs() {
      return args;
    }

    public String getInsertFields() {
      return insertFields;
    }

    public String getInsertParams() {
      return insertParams;
    }
  }

  /**
   * Field and values for update queries.
   */
  private static class UpdateData {
    private final Object[] args;
    private final String[] fields;

    /**
     * @param key    Key.
     * @param values Field values.
     */
    UpdateData(String key, Map<String, ByteIterator> values) {
      args = new String[values.size() + 1];
      fields = new String[values.size()];

      int idx = 0;

      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        args[idx++] = e.getValue().toString();
        fields[idx++] = e.getKey();
      }

      args[idx] = key;
    }

    public Object[] getArgs() {
      return args;
    }

    public String[] getFields() {
      return fields;
    }
  }
}
