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
import com.yahoo.ycsb.db.flavors.DBFlavor;
import com.yahoo.ycsb.db.flavors.DefaultDBFlavor;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static Ignite cluster = null;
  private static IgniteCache<String, BinaryObject> cache = null;
  private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
  private static boolean debug = false;
  private DBFlavor dbFlavor = new DefaultDBFlavor();
  private ConcurrentMap<StatementType, SqlFieldsQuery> cachedQuery = new ConcurrentHashMap<>();

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

//        igcfg.setLocalHost(host);

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
      StatementType type = new StatementType(StatementType.Type.READ, table, 1, "", 0);

      SqlFieldsQuery qry = cachedQuery.get(type);
      if (qry == null) {
        qry = createAndCacheReadStatement(type, key);
      }
      qry.setArgs(key);

      FieldsQueryCursor<List<?>> cur = cache.query(qry);
      Iterator<List<?>> it = cur.iterator();

      if (!it.hasNext()) {
        return Status.NOT_FOUND;
      }

      String [] colNames = new String[cur.getColumnsCount()];
      for(int i = 0; i < colNames.length; ++i) {
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

      while(it.hasNext()) {
        List<?> row = it.next();

        for(int i = 0; i < colNames.length; ++i) {
          if (colNames[i] != null) {
            result.put(colNames[i], new StringByteIterator((String)row.get(i)));
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
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, table, numFields, fieldInfo.getFieldKeys(), 0);

      SqlFieldsQuery qry = cachedQuery.get(type);
      if (qry == null) {
        qry = createAndCacheUpdateStatement(type, key);
      }

      Object[] args = new Object[numFields + 1];
      args[0] = key;
      int index = 1;
      for (String value : fieldInfo.getFieldValues()) {
        args[index++] = value;
      }
      qry.setArgs(args);

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
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, table, numFields, fieldInfo.getFieldKeys(), 0);

      SqlFieldsQuery qry = cachedQuery.get(type);
      if (qry == null) {
        qry = createAndCacheInsertStatement(type, key);
      }

      Object[] args = new Object[numFields + 1];
      args[0] = key;
      int index = 1;
      for (String value : fieldInfo.getFieldValues()) {
        args[index++] = value;
      }
      qry.setArgs(args);

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
      StatementType type = new StatementType(StatementType.Type.DELETE, table, 1, "", 0);

      SqlFieldsQuery qry = cachedQuery.get(type);
      if (qry == null) {
        qry = createAndCacheDeleteStatement(type, key);
      }
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
   * @param type Query type.
   * @param key Key.
   * @return Query.
   */
  private SqlFieldsQuery createAndCacheReadStatement(StatementType type, String key) {
    String insert = dbFlavor.createReadStatement(type, key);
    SqlFieldsQuery newQry = new SqlFieldsQuery(insert);
    SqlFieldsQuery qry = cachedQuery.putIfAbsent(type, newQry);
    if (qry == null) {
      return newQry;
    }
    return qry;
  }

  /**
   * @param type Query type.
   * @param key Key.
   * @return Query.
   */
  private SqlFieldsQuery createAndCacheUpdateStatement(StatementType type, String key) {
    String insert = dbFlavor.createUpdateStatement(type, key);
    SqlFieldsQuery newQry = new SqlFieldsQuery(insert);
    SqlFieldsQuery qry = cachedQuery.putIfAbsent(type, newQry);
    if (qry == null) {
      return newQry;
    }
    return qry;
  }

  /**
   * @param type Query type.
   * @param key Key.
   * @return Query.
   */
  private SqlFieldsQuery createAndCacheInsertStatement(StatementType type, String key) {
    String insert = dbFlavor.createInsertStatement(type, key);
    SqlFieldsQuery newQry = new SqlFieldsQuery(insert);
    SqlFieldsQuery qry = cachedQuery.putIfAbsent(type, newQry);
    if (qry == null) {
      return newQry;
    }
    return qry;
  }

  /**
   * @param type Query type.
   * @param key Key.
   * @return Query.
   */
  private SqlFieldsQuery createAndCacheDeleteStatement(StatementType type, String key) {
    String insert = dbFlavor.createDeleteStatement(type, key);
    SqlFieldsQuery newQry = new SqlFieldsQuery(insert);
    SqlFieldsQuery qry = cachedQuery.putIfAbsent(type, newQry);
    if (qry == null) {
      return newQry;
    }
    return qry;
  }

  /**
   * @param values YCSB values representation.
   * @return Internal fields info.
   */
  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    /**
     * @param fieldKeys Keys string.
     * @param fieldValues Values.
     */
    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    /**
     * @return Keys.
     */
    String getFieldKeys() {
      return fieldKeys;
    }

    /**
     * @return Values.
     */
    List<String> getFieldValues() {
      return fieldValues;
    }
  }
}
