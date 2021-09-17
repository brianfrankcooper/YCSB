/*
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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
 */
package site.ycsb.db.scylla;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.*;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * Scylla DB implementation.
 */
public class ScyllaCQLClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaCQLClient.class);

  private static Cluster cluster = null;
  private static Session session = null;

  private static final ConcurrentMap<Set<String>, PreparedStatement> READ_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> SCAN_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> INSERT_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> UPDATE_STMTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> READ_ALL_STMT = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> SCAN_ALL_STMT = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> DELETE_STMT = new AtomicReference<>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;

  private static boolean lwt = false;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "scylla.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "scylla.username";
  public static final String PASSWORD_PROPERTY = "scylla.password";

  public static final String HOSTS_PROPERTY = "scylla.hosts";
  public static final String PORT_PROPERTY = "scylla.port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "scylla.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "scylla.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();

  public static final String MAX_CONNECTIONS_PROPERTY = "scylla.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "scylla.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "scylla.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "scylla.readtimeoutmillis";

  public static final String SCYLLA_LWT = "scylla.lwt";

  public static final String TOKEN_AWARE_LOCAL_DC = "scylla.local_dc";

  public static final String TRACING_PROPERTY = "scylla.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "scylla.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;

  private static boolean trace = false;

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
        trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format("Required property \"%s\" missing for scyllaCQLClient", HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        boolean useSSL = Boolean.parseBoolean(
            getProperties().getProperty(USE_SSL_CONNECTION, DEFAULT_USE_SSL_CONNECTION));

        Cluster.Builder builder;
        if ((username != null) && !username.isEmpty()) {
          builder = Cluster.builder().withCredentials(username, password)
              .addContactPoints(hosts).withPort(Integer.parseInt(port));
          if (useSSL) {
            builder = builder.withSSL();
          }
        } else {
          builder = Cluster.builder().withPort(Integer.parseInt(port))
              .addContactPoints(hosts);
        }

        final String localDC = getProperties().getProperty(TOKEN_AWARE_LOCAL_DC);
        if (localDC != null && !localDC.isEmpty()) {
          final LoadBalancingPolicy local = DCAwareRoundRobinPolicy.builder().withLocalDc(localDC).build();
          final TokenAwarePolicy tokenAware = new TokenAwarePolicy(local);
          builder = builder.withLoadBalancingPolicy(tokenAware);

          LOGGER.info("Using local datacenter with token awareness: {}\n", localDC);

          // If was not overridden explicitly, set LOCAL_QUORUM
          if (getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY) == null) {
            readConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
          }

          if (getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY) == null) {
            writeConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
          }
        }

        cluster = builder.build();

        String maxConnections = getProperties().getProperty(
            MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(maxConnections));
        }

        String coreConnections = getProperties().getProperty(
            CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(coreConnections));
        }

        String connectTimeoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimeoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setConnectTimeoutMillis(Integer.parseInt(connectTimeoutMillis));
        }

        String readTimeoutMillis = getProperties().getProperty(
            READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimeoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setReadTimeoutMillis(Integer.parseInt(readTimeoutMillis));
        }

        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}\n", metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          LOGGER.info("Datacenter: {}; Host: {}; Rack: {}\n",
              discoveredHost.getDatacenter(), discoveredHost.getEndPoint().resolve().getAddress(),
              discoveredHost.getRack());
        }

        session = cluster.connect(keyspace);

        if (Boolean.parseBoolean(getProperties().getProperty(SCYLLA_LWT, Boolean.toString(lwt)))) {
          LOGGER.info("Using LWT\n");
          lwt = true;
          readConsistencyLevel = ConsistencyLevel.SERIAL;
          writeConsistencyLevel = ConsistencyLevel.ANY;
        } else {
          LOGGER.info("Not using LWT\n");
        }

        LOGGER.info("Read consistency: {}, Write consistency: {}\n",
            readConsistencyLevel.name(),
            writeConsistencyLevel.name());
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
        READ_STMTS.clear();
        SCAN_STMTS.clear();
        INSERT_STMTS.clear();
        UPDATE_STMTS.clear();
        READ_ALL_STMT.set(null);
        SCAN_ALL_STMT.set(null);
        DELETE_STMT.set(null);
        if (session != null) {
          session.close();
          session = null;
        }
        if (cluster != null) {
          cluster.close();
          cluster = null;
        }
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
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
                     Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? READ_ALL_STMT.get() : READ_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        stmt = session.prepare(selectBuilder.from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
            .limit(1));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
            READ_ALL_STMT.getAndSet(stmt) :
            READ_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQueryString());
      LOGGER.debug("key = {}", key);

      ResultSet rs = session.execute(stmt.bind(key));

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
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

      return Status.OK;

    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * scylla CQL uses "token" method for range scan which doesn't always yield
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
      PreparedStatement stmt = (fields == null) ? SCAN_ALL_STMT.get() : SCAN_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        Select selectStmt = selectBuilder.from(table);

        // The statement builder is not setup right for tokens.
        // So, we need to build it manually.
        String initialStmt = selectStmt.toString();
        String scanStmt = initialStmt.substring(0, initialStmt.length() - 1) +
            " WHERE " + QueryBuilder.token(YCSB_KEY) +
            " >= token(" + QueryBuilder.bindMarker() + ")" +
            " LIMIT " + QueryBuilder.bindMarker();
        stmt = session.prepare(scanStmt);
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
            SCAN_ALL_STMT.getAndSet(stmt) :
            SCAN_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQueryString());
      LOGGER.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      ResultSet rs = session.execute(stmt.bind(startkey, recordcount));

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<>();

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
      LOGGER.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
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
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = UPDATE_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Update updateStmt = QueryBuilder.update(table);

        // Add fields
        for (String field : fields) {
          updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        }

        // Add key
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        if (lwt) {
          updateStmt.where().ifExists();
        }

        stmt = session.prepare(updateStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = UPDATE_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(stmt.getQueryString());
        LOGGER.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 1; i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      // Add key
      boundStmt.setString(vars.size() - 1, key);

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
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
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = INSERT_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Insert insertStmt = QueryBuilder.insertInto(table);

        // Add key
        insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add fields
        for (String field : fields) {
          insertStmt.value(field, QueryBuilder.bindMarker());
        }

        if (lwt) {
          insertStmt.ifNotExists();
        }

        stmt = session.prepare(insertStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = INSERT_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(stmt.getQueryString());
        LOGGER.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      BoundStatement boundStmt = stmt.bind().setString(0, key);

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      for (int i = 1; i < vars.size(); i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
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
      PreparedStatement stmt = DELETE_STMT.get();

      // Prepare statement on demand
      if (stmt == null) {
        Delete s = QueryBuilder.delete().from(table);
        s.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        if (lwt) {
          s.ifExists();
        }

        stmt = session.prepare(s);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = DELETE_STMT.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQueryString());
      LOGGER.debug("key = {}", key);

      session.execute(stmt.bind(key));

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

}
