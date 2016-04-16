/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.yahoo.ycsb.*;
import java.nio.ByteBuffer;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tested with Cassandra 2.0, CQL client for YCSB framework
 *
 * See {@code cassandra2} for a version compatible with Cassandra 2.1+. See
 * {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Cluster cluster = null;
  private static Session session = null;

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

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

        debug =
            Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty("port", "9042");
        if (port == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              PORT_PROPERTY));
        }

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        // public void connect(String node) {}
        if ((username != null) && !username.isEmpty()) {
          cluster = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.valueOf(port)).addContactPoints(hosts).build();
        } else {
          cluster = Cluster.builder().withPort(Integer.valueOf(port))
              .addContactPoints(hosts).build();
        }

        // Update number of connections based on threads
        int threadcount =
            Integer.parseInt(getProperties().getProperty("threadcount", "1"));
        cluster.getConfiguration().getPoolingOptions()
            .setMaxConnectionsPerHost(HostDistance.LOCAL, threadcount);

        // Set connection timeout 3min (default is 5s)
        cluster.getConfiguration().getSocketOptions()
            .setConnectTimeoutMillis(3 * 60 * 1000);
        // Set read (execute) timeout 3min (default is 12s)
        cluster.getConfiguration().getSocketOptions()
            .setReadTimeoutMillis(3 * 60 * 1000);

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
            metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
              discoveredHost.getDatacenter(), discoveredHost.getAddress(),
              discoveredHost.getRack());
        }

        session = cluster.connect(keyspace);

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
    if (INIT_COUNT.decrementAndGet() <= 0) {
      cluster.shutdown();
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {

    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(table).where(QueryBuilder.eq(YCSB_KEY, key))
          .limit(1);
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      ResultSet rs = session.execute(stmt);

      // Should be only 1 row
      if (!rs.isExhausted()) {
        Row row = rs.one();
        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            result.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            result.put(def.getName(), null);
          }
        }

      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(table);

      // The statement builder is not setup right for tokens.
      // So, we need to build it manually.
      String initialStmt = stmt.toString();
      StringBuilder scanStmt = new StringBuilder();
      scanStmt.append(initialStmt.substring(0, initialStmt.length() - 1));
      scanStmt.append(" WHERE ");
      scanStmt.append(QueryBuilder.token(YCSB_KEY));
      scanStmt.append(" >= ");
      scanStmt.append("token('");
      scanStmt.append(startkey);
      scanStmt.append("')");
      scanStmt.append(" LIMIT ");
      scanStmt.append(recordcount);

      stmt = new SimpleStatement(scanStmt.toString());
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      ResultSet rs = session.execute(stmt);

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<String, ByteIterator>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }

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
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    // Insert and updates provide the same functionality
    return insert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {

    try {
      Insert insertStmt = QueryBuilder.insertInto(table);

      // Add key
      insertStmt.value(YCSB_KEY, key);

      // Add fields
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        Object value;
        ByteIterator byteIterator = entry.getValue();
        value = byteIterator.toString();

        insertStmt.value(entry.getKey(), value);
      }

      insertStmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(insertStmt.toString());
      }

      session.execute(insertStmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      Statement stmt;

      stmt = QueryBuilder.delete().from(table)
          .where(QueryBuilder.eq(YCSB_KEY, key));
      stmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      session.execute(stmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error deleting key: " + key);
    }

    return Status.ERROR;
  }

}
