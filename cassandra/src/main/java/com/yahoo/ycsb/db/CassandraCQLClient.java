/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
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
import com.yahoo.ycsb.workloads.CoreWorkload;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static final Object PREPARED_INIT_COORDINATOR = new Object();

  /**
   * Properties and their defaults.
   */
  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";
  private static final String FIELD_PREFIX = "field";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String MAX_CONNECTIONS_PROPERTY =
      "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY =
      "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.readtimeoutmillis";

  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = Boolean.FALSE.toString();

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static Cluster cluster;
  private static Session session;
  private static List<String> fieldList;

  private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

  // YCSB always inserts a full row, but updates can be either full-row or single-column
  private static PreparedStatement insertStatement;
  private static Map<String, PreparedStatement> updateStatements;

  private static PreparedStatement deleteStatement;

  /*
    select and scan statements have two variants;
    one to select all columns, and one for selecting each individual column
  */
  private static PreparedStatement selectStatement;
  private static Map<String, PreparedStatement> singleSelectStatements;
  private static PreparedStatement scanStatement;
  private static Map<String, PreparedStatement> singleScanStatements;

  private static Map<Integer, Map<Integer, PreparedStatement>> selectManyStatements;
  private static Map<Integer, Map<Integer, PreparedStatement>> scanManyStatements;

  private boolean readallfields;

  private boolean debug = false;
  private boolean trace = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    final Properties p = getProperties();

    debug =
      Boolean.parseBoolean(p.getProperty("debug", Boolean.FALSE.toString()));

    String host = p.getProperty(HOSTS_PROPERTY);
    if (host == null) {
      throw new DBException(String.format(
        "Required property \"%s\" missing for CassandraCQLClient",
        HOSTS_PROPERTY));
    }
    String[] hosts = host.split(",");
    String port = p.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

    String username = p.getProperty(USERNAME_PROPERTY);
    String password = p.getProperty(PASSWORD_PROPERTY);

    String keyspace = p.getProperty(KEYSPACE_PROPERTY,
        KEYSPACE_PROPERTY_DEFAULT);

    readConsistencyLevel = ConsistencyLevel.valueOf(
      p.getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
        READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
    writeConsistencyLevel = ConsistencyLevel.valueOf(
      p.getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
        WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

    readallfields = Boolean.parseBoolean(
      p.getProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY,
        CoreWorkload.READ_ALL_FIELDS_PROPERTY_DEFAULT));

    trace = Boolean.valueOf(p.getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }

      try {

        if ((username != null) && !username.isEmpty()) {
          cluster = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.valueOf(port)).addContactPoints(hosts).build();
        } else {
          cluster = Cluster.builder().withPort(Integer.valueOf(port))
              .addContactPoints(hosts).build();
        }

        String maxConnections = p.getProperty(
            MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(maxConnections));
        }

        String coreConnections = p.getProperty(
            CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(coreConnections));
        }

        String connectTimeoutMillis = p.getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimeoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setConnectTimeoutMillis(Integer.valueOf(connectTimeoutMillis));
        }

        String readTimeoutMillis = p.getProperty(
            READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimeoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setReadTimeoutMillis(Integer.valueOf(readTimeoutMillis));
        }

        session = cluster.connect(keyspace);

        Metadata metadata = cluster.getMetadata();
        System.err.printf("Connected to cluster: %s\n",
            metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
              discoveredHost.getDatacenter(), discoveredHost.getAddress(),
              discoveredHost.getRack());
        }

        buildStatements(p);

      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  private void buildStatements(Properties p) {
    String table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);

    int fieldCount = Integer.parseInt(
        p.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    fieldList = Collections.synchronizedList(new ArrayList<String>(fieldCount));

    if (!readallfields) {
      singleSelectStatements = new ConcurrentHashMap<>(fieldCount);
      singleScanStatements = new ConcurrentHashMap<>(fieldCount);

      selectManyStatements = new ConcurrentSkipListMap<>();
      scanManyStatements = new ConcurrentSkipListMap<>();
    }

    // Insert and Update all statement
    final Insert is = QueryBuilder.insertInto(table);
    is.value(YCSB_KEY, QueryBuilder.bindMarker());

    updateStatements = new ConcurrentHashMap<>(fieldCount);

    fieldList.add(YCSB_KEY);
    for (int i = 0; i < fieldCount; i++) {
      // Preserve field iteration order for the HashMaps used in the DB methods
      final String field = FIELD_PREFIX + i;
      fieldList.add(field);

      // Insert and Update statement
      is.value(field, QueryBuilder.bindMarker());

      // Update - single
      Insert updateOneStmt = QueryBuilder.insertInto(table)
          .values(
              new String[] {YCSB_KEY, field},
              new Object[] {QueryBuilder.bindMarker(), QueryBuilder.bindMarker()});

      if (!updateStatements.containsKey(field)) {
        updateStatements.put(field, prepare(updateOneStmt.getQueryString(), false));
      }

      // Select and Scan few statements
      if (!readallfields) {
        // Select - single
        Select selectOne = QueryBuilder.select(field).from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
            .limit(1);

        if (!singleSelectStatements.containsKey(field)) {
          singleSelectStatements.put(field, prepare(selectOne.getQueryString(), true));
        }

        // Scan - single
        String initialScanStmt = QueryBuilder.select(field).from(table).toString();
        String scanOneStmt = getScanQueryString().replaceFirst("_",
            initialScanStmt.substring(0, initialScanStmt.length() - 1));

        if (!singleScanStatements.containsKey(field)) {
          singleScanStatements.put(field, prepare(scanOneStmt, true));
        }

        // Select and Scan many statements
        selectManyStatements.put(i, new ConcurrentHashMap<Integer, PreparedStatement>());
        scanManyStatements.put(i, new ConcurrentHashMap<Integer, PreparedStatement>());
      }
    }

    // Prepare insert and update all
    insertStatement = prepare(is.getQueryString(), false);

    // Select All
    String ss = QueryBuilder.select().all().from(table)
        .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).getQueryString();
    selectStatement = prepare(ss, true);

    // Scan All
    String initialStmt = QueryBuilder.select().all().from(table).toString();
    String scanStmt = getScanQueryString().replaceFirst("_",
        initialStmt.substring(0, initialStmt.length() - 1));
    scanStatement = prepare(scanStmt, true);

    // Delete on key statement
    String ds = QueryBuilder.delete().from(table)
        .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).toString();
    deleteStatement = prepare(ds, false);
  }

  private PreparedStatement prepare(String statement, boolean isRead) {
    if (debug) {
      System.out.printf("Preparing '%s'\n", statement);
    }
    PreparedStatement prepared = session.prepare(statement);
    prepared.setConsistencyLevel(isRead ? readConsistencyLevel : writeConsistencyLevel);
    return prepared;
  }

  private String getScanQueryString() {
    return String.format("_ WHERE %s >= token(%s) LIMIT %s;",
            QueryBuilder.token(YCSB_KEY), QueryBuilder.bindMarker(), QueryBuilder.bindMarker());
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
        session.close();
        cluster.close();
        cluster = null;
        session = null;
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

    PreparedStatement ps;

    try {
      if (readallfields || fields == null) {
        ps = selectStatement;
      } else if (fields.size() == 1) {
        ps = singleSelectStatements.get(fields.iterator().next());
      } else {
        Map<Integer, PreparedStatement> possibleStatements = selectManyStatements.get(fields.size());

        Select.Builder selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
        String statement = selectBuilder.from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).getQueryString();

        int statementHash = statement.hashCode(); // Hoping these are unique enough between statements
        if (!possibleStatements.containsKey(statementHash)) {
          possibleStatements.put(statementHash,  prepare(statement, true));
        }
        ps = possibleStatements.get(statementHash);
      }

      if (ps == null) {
        throw new RuntimeException("read: Unable to get PreparedStatement.");
      }
      final BoundStatement bs = ps.bind(key);

      if (trace) {
        bs.enableTracing();
      }
      if (debug) {
        System.out.println(">> " + getBoundQuery(bs));
      }
      
      ResultSet rs = session.execute(bs);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();

      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        result.put(def.getName(), val == null ? null : new ByteArrayByteIterator(val.array()));
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }

  }

  private static String getBoundQuery(BoundStatement bs) {
    String q = bs.preparedStatement().getQueryString();
    System.out.print(q);
    ColumnDefinitions variables = bs.preparedStatement().getVariables();
    int index = 0;
    for (ColumnDefinitions.Definition variable : variables) {
      DataType type = variable.getType();
      String value = String.valueOf(bs.getObject(index++));
      // Tried to in-line the values, but wasn't working too well
      System.out.printf(" [(%s) %s]", type, value);
    }
    System.out.println();
    return q;
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

    PreparedStatement ps;

    try {
      if (readallfields || fields == null) {
        ps = scanStatement;
      } else if (fields.size() == 1) {
        ps = singleScanStatements.get(fields.iterator().next());
      } else {
        Map<Integer, PreparedStatement> possibleStatements = scanManyStatements.get(fields.size());

        Select.Builder selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }

        String stmt = selectBuilder.from(table).getQueryString();
        String scanStmt = getScanQueryString().replaceFirst("_",
            stmt.substring(0, stmt.length()-1));

        int statementHash = scanStmt.hashCode(); // Hoping these are unique enough between statements
        if (!possibleStatements.containsKey(statementHash)) {
          possibleStatements.put(statementHash, prepare(scanStmt, true));
        }
        ps = possibleStatements.get(statementHash);
      }

      if (ps == null) {
        throw new RuntimeException("scan: Unable to get PreparedStatement.");
      }

      final BoundStatement bs = ps.bind(startkey, recordcount);

      if (trace) {
        bs.enableTracing();
      }
      if (debug) {
        System.out.println(">> " + getBoundQuery(bs));
      }

      ResultSet rs = session.execute(bs);

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          tuple.put(def.getName(), val == null ? null : new ByteArrayByteIterator(val.array()));
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Error scanning with startkey: " + startkey);
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
                       Map<String, ByteIterator> values) {
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
      Map<String, ByteIterator> values) {

    try {
      final int numValues = values.size();
      int i = 0;
      Object[] vals = new Object[numValues + 1];

      // Add key
      vals[i++] = key;

      // Add fieldList
      for (int j = i; j < fieldList.size(); j++) {
        String field = fieldList.get(j);
        if (values.containsKey(field)) {
          vals[i++] = values.get(field).toString();
        }
      }

      // If there is one value, this is actually an update
      BoundStatement bs = (numValues == 1 ?
               updateStatements.get(values.keySet().iterator().next()) :
               insertStatement).bind(vals);

      if (trace) {
        bs.enableTracing();
      }
      if (debug) {
        System.out.println(">> " + getBoundQuery(bs));
      }
      
      session.execute(bs);

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

      BoundStatement bs = deleteStatement.bind(key);
      if (trace) {
        deleteStatement.enableTracing();
      }
      if (debug) {
        System.out.println(">> " + getBoundQuery(bs));
      }

      session.execute(bs);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error deleting key: " + key);
    }

    return Status.ERROR;
  }

}
