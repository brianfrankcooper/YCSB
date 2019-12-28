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
package site.ycsb.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.TimeseriesDB;
import site.ycsb.workloads.CoreWorkload;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.gson.Gson;


/**
 * Cassandra 2.x CQL client for Time Series Workloads
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author smartygus
 */
public class CassandraCQLClientTS extends TimeseriesDB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClientTS.class);

  private static Cluster cluster = null;
  private static Session session = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static AtomicReference<PreparedStatement> readAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> scanAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> deleteStmt =
      new AtomicReference<PreparedStatement>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

  public static final String YCSB_KEY = "metric";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  /**
   * The name of the property used to lookup the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "metrics";

  protected String table;

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
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
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
    // Call init() from TimeseriesDB to ensure correct 
    // workload parsing
    super.init();

    // Get table name from properties (if it exists, otherwise use default)
    table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

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
        if (debug) {
          logger.info("Workload Properties: \n" + getProperties());
        }
        trace = Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

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

        Boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION,
            DEFAULT_USE_SSL_CONNECTION));

        if ((username != null) && !username.isEmpty()) {
          Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.valueOf(port)).addContactPoints(hosts);
          if (useSSL) {
            clusterBuilder = clusterBuilder.withSSL();
          } 
          cluster = clusterBuilder.build();
        } else {
          cluster = Cluster.builder().withPort(Integer.valueOf(port))
              .addContactPoints(hosts).build();
        }

        // Setup query logger if trace is enabled
        if (trace) {
          QueryLogger queryLogger = QueryLogger.builder()
            .withMaxQueryStringLength(256)
            .build();
          cluster.register(queryLogger);
        }

        String maxConnections = getProperties().getProperty(
            MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(maxConnections));
        }

        String coreConnections = getProperties().getProperty(
            CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(coreConnections));
        }

        String connectTimoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setConnectTimeoutMillis(Integer.valueOf(connectTimoutMillis));
        }

        String readTimoutMillis = getProperties().getProperty(
            READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setReadTimeoutMillis(Integer.valueOf(readTimoutMillis));
        }

        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}\n",
            metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          logger.info("Datacenter: {}; Host: {}; Rack: {}\n",
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
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        scanAllStmt.set(null);
        deleteStmt.set(null);
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
   * be stored in a HashMap. (except in TimeseriesDB no reference is passed
   * for storing results??)
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to read.
   * @param tags      actual tags that were want to receive (can be empty)
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String metric, long timestamp, Map<String, List<String>> tags) {
    try {
      Map<String, String> tagsMap = new HashMap();
      // Tags are passed as a Map with the values being a list
      // (for some reason, even though tags should only have
      // one value?)
      // Here we pull the value out of the array so that we
      // have a simple map.
      for(Map.Entry<String, List<String>> entry : tags.entrySet()) {
        tagsMap.put(entry.getKey(), ((List<String>)entry.getValue()).get(0));
      }
      String tagsQueryAsJson = new Gson().toJson(tagsMap);
      if (debug) {
        logger.info("[READ][parameters] metric: " + metric + ", timestamp: " + timestamp + ", tags: " + tagsQueryAsJson);
      }
      Set<String> queryFields = new HashSet();
      queryFields.add("value");
      

      PreparedStatement stmt = readStmts.get(queryFields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        selectBuilder = QueryBuilder.select();
        // We really only want the value back from the query I guess
        ((Select.Selection) selectBuilder).column("value");
        
        stmt = session.prepare(selectBuilder.from(table)
                               .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
                               .and(QueryBuilder.eq("valuetime", QueryBuilder.bindMarker()))
                               .and(QueryBuilder.eq("tags", QueryBuilder.bindMarker()))
                               .limit(1));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = readStmts.putIfAbsent(new HashSet(queryFields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      if (debug) {
        logger.info("[READ][query string] " + stmt.getQueryString());
      }

      // Add metric
      BoundStatement boundStmt = stmt.bind().setString(YCSB_KEY, metric);

      // Add timestamp
      Date timestampDate = new Date(timestamp * 1000);
      boundStmt.setTimestamp("valuetime", timestampDate);

      // Add tags
      boundStmt.setString("tags", tagsQueryAsJson);
      
      ResultSet rs = session.execute(boundStmt);

      if (rs.isExhausted()) {
        if (debug) {
          logger.info("[READ][NOT FOUND]\n\n");
        }
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();
      Double resultValue = row.getDouble("value");
      if (debug) {
        logger.info("[READ][result] value: " + resultValue);
      }

      //for (ColumnDefinitions.Definition def : cd) {
        //Double val = row.getDouble(def.getName());
        //if (val != null) {
          //logger.info("[READ][result] " + def.getName() + ": " + val);
          ////result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        //} else {
          //logger.info("[READ][result] " + def.getName() + ": [NULL VALUE RETURNED]");
          ////result.put(def.getName(), null);
        //}
      //}

      return Status.OK;

    } catch (Exception e) {
      logger.error(MessageFormatter.arrayFormat("Error reading metric: {}, tags: {}, timestamp: {}", new Object[]{metric, new Gson().toJson(tags), timestamp}).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each value from the result will be stored in a
   * HashMap.
   *
   * @param metric    The name of the metric
   * @param startTs   The timestamp of the first record to read.
   * @param endTs     The timestamp of the last record to read.
   * @param tags      actual tags that were want to receive (can be empty).
   * @param aggreg    The aggregation operation to perform.
   * @param timeValue value for timeUnit for aggregation
   * @param timeUnit  timeUnit for aggregation
   * @return A {@link Status} detailing the outcome of the scan operation.
   */
  @Override
  public Status scan(String metric, long startTs, long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    try {
      Map<String, String> tagsMap = new HashMap();
      // Tags are passed as a Map with the values being a list
      // (for some reason, even though tags should only have
      // one value?)
      // Here we pull the value out of the array so that we
      // have a simple map.
      for(Map.Entry<String, List<String>> entry : tags.entrySet()) {
        tagsMap.put(entry.getKey(), ((List<String>)entry.getValue()).get(0));
      }
      String tagsQueryAsJson = new Gson().toJson(tagsMap);
      if (debug) {
        logger.info(">>[SCAN] metric: " + metric + ", startTs: " + startTs + ", endTs: " + endTs + ", tags: " + tagsQueryAsJson + ", aggreg: " + aggreg + ", timeValue: " + timeValue + ", timeUnit: " + timeUnit + "<<");
      }


      Set<String> queryFields = new HashSet();
      queryFields.add("valuetime");
      queryFields.add("value");
      

      PreparedStatement stmt = scanStmts.get(queryFields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        selectBuilder = QueryBuilder.select();
        ((Select.Selection) selectBuilder).column("valuetime");
        ((Select.Selection) selectBuilder).column("value");
        
        stmt = session.prepare(selectBuilder.from(table)
                               .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
                               .and(QueryBuilder.eq("tags", QueryBuilder.bindMarker()))
                               .and(QueryBuilder.gte("valuetime", QueryBuilder.bindMarker("startTs")))
                               .and(QueryBuilder.lte("valuetime", QueryBuilder.bindMarker("endTs"))));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = scanStmts.putIfAbsent(new HashSet(queryFields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      if (debug) {
        logger.info("[SCAN][query string] " + stmt.getQueryString());
      }

      // Add metric
      BoundStatement boundStmt = stmt.bind().setString(YCSB_KEY, metric);

      // Add timestamps
      Date startTimestampDate = new Date(startTs* 1000);
      Date endTimestampDate = new Date(endTs* 1000);
      boundStmt.setTimestamp("startTs", startTimestampDate);
      boundStmt.setTimestamp("endTs", endTimestampDate);

      // Add tags
      boundStmt.setString("tags", tagsQueryAsJson);
      
      ResultSet rs = session.execute(boundStmt);

      if (rs.isExhausted()) {
        if (debug) {
          logger.info("[SCAN][NOT FOUND]\n\n");
        }
        return Status.NOT_FOUND;
      }

      for (Row row : rs) {
        Date resultTimestamp = row.getTimestamp("valuetime");
        Double resultValue = row.getDouble("value");
        if (debug) {
          logger.info("[SCAN][result] timestamp: (date: " + resultTimestamp + ", unix: " + resultTimestamp.getTime() + "), value: " + resultValue);
        }
      }


      return Status.OK;

    } catch (Exception e) {
      logger.error(
          MessageFormatter.format("Error scanning with metric: {}", metric).getMessage(), e);
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
  //@Override
  //public Status update(String table, String key, Map<String, ByteIterator> values) {

  //  try {
  //    Set<String> fields = values.keySet();
  //    PreparedStatement stmt = updateStmts.get(fields);

  //    // Prepare statement on demand
  //    if (stmt == null) {
  //      Update updateStmt = QueryBuilder.update(table);

  //      // Add fields
  //      for (String field : fields) {
  //        updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
  //      }

  //      // Add key
  //      updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

  //      stmt = session.prepare(updateStmt);
  //      stmt.setConsistencyLevel(writeConsistencyLevel);
  //      if (trace) {
  //        stmt.enableTracing();
  //      }

  //      PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet(fields), stmt);
  //      if (prevStmt != null) {
  //        stmt = prevStmt;
  //      }
  //    }

  //    if (logger.isDebugEnabled()) {
  //      logger.debug(stmt.getQueryString());
  //      logger.debug("key = {}", key);
  //      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
  //        logger.debug("{} = {}", entry.getKey(), entry.getValue());
  //      }
  //    }

  //    // Add fields
  //    ColumnDefinitions vars = stmt.getVariables();
  //    BoundStatement boundStmt = stmt.bind();
  //    for (int i = 0; i < vars.size() - 1; i++) {
  //      boundStmt.setString(i, values.get(vars.getName(i)).toString());
  //    }

  //    // Add key
  //    boundStmt.setString(vars.size() - 1, key);

  //    session.execute(boundStmt);

  //    return Status.OK;
  //  } catch (Exception e) {
  //    logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
  //  }

    //return Status.ERROR;
 //}


  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to insert.
   * @param value     The actual value to insert.
   * @param tags      A Map of tag/tagvalue pairs to insert as tags
   * @return A {@link Status} detailing the outcome of the insert - Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String metric, long timestamp, long value, Map<String, ByteIterator> tags) {
    return insert(metric, timestamp, (double)value, tags);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to insert.
   * @param value     The actual value to insert.
   * @param tags      A Map of tag/tagvalue pairs to insert as tags
   * @return A {@link Status} detailing the outcome of the insert - Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String metric, long timestamp, double value, Map<String, ByteIterator> tags) {
    
    try {
      Map<String, String> tagsAsStrings = new HashMap();
      for (Map.Entry entry : tags.entrySet()) {
        tagsAsStrings.put(entry.getKey().toString(), entry.getValue().toString());
      }
      if (debug) {
        logger.info("[INSERT] metric: " + metric + ", timestamp: " + timestamp + ", value: " + value + ", tags: " + new Gson().toJson(tagsAsStrings));
      }

      Set<String> queryFields = new HashSet();
      queryFields.add("metric");
      queryFields.add("tags");
      queryFields.add("valuetime");
      queryFields.add("value");
      
      PreparedStatement stmt = insertStmts.get(queryFields);

      // Prepare statement on demand
      if (stmt == null) {
        Insert insertStmt = QueryBuilder.insertInto(table);

        // Add metric
        insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add timestamp
        insertStmt.value("valuetime", QueryBuilder.bindMarker());

        // Add tags
        insertStmt.value("tags", QueryBuilder.bindMarker());

        // Add value
        insertStmt.value("value", QueryBuilder.bindMarker());
        
        stmt = session.prepare(insertStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(queryFields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      // Add metric
      BoundStatement boundStmt = stmt.bind().setString(YCSB_KEY, metric);

      // Add timestamp
      Date timestampDate = new Date(timestamp * 1000);
      boundStmt.setTimestamp("valuetime", timestampDate);

      // Add tags
      boundStmt.setString("tags", new Gson().toJson(tagsAsStrings));

      // Add value
      boundStmt.setDouble("value", value);

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.arrayFormat("Error inserting metric: {}, tags: {}, timestamp: {}, value: {}", new Object[]{metric, new Gson().toJson(tags), timestamp, value}).getMessage(), e);
      return Status.ERROR;
    }
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

    //try {
      //PreparedStatement stmt = deleteStmt.get();

      //// Prepare statement on demand
      //if (stmt == null) {
        //stmt = session.prepare(QueryBuilder.delete().from(table)
                               //.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));
        //stmt.setConsistencyLevel(writeConsistencyLevel);
        //if (trace) {
          //stmt.enableTracing();
        //}

        //PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        //if (prevStmt != null) {
          //stmt = prevStmt;
        //}
      //}

      //logger.debug(stmt.getQueryString());
      //logger.debug("key = {}", key);

      //session.execute(stmt.bind(key));

      //return Status.OK;
    //} catch (Exception e) {
      //logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    //}

    return Status.ERROR;
  }

}
