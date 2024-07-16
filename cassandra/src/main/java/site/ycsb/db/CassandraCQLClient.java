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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.*;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  private static CqlSession session = null;
  private static DriverExecutionProfile readProfile = null;
  private static DriverExecutionProfile scanProfile = null;
  private static DriverExecutionProfile insertProfile = null;
  private static DriverExecutionProfile updateProfile = null;
  private static DriverExecutionProfile deleteProfile = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private static AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();
  private static AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<>();
  private static AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;

  public static final String YCSB_KEY = "y_id";

  public static final String DRIVER_CONFIG_PROPERTY = "cassandra.driverconfig";
  public static final String DRIVER_PROFILE_READ_PROPERTY = "cassandra.driverprofile.read";
  public static final String DRIVER_PROFILE_INSERT_PROPERTY = "cassandra.driverprofile.insert";
  public static final String DRIVER_PROFILE_SCAN_PROPERTY = "cassandra.driverprofile.scan";
  public static final String DRIVER_PROFILE_DELETE_PROPERTY = "cassandra.driverprofile.delete";
  public static final String DRIVER_PROFILE_UPDATE_PROPERTY = "cassandra.driverprofile.update";
  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_FREQUENCY_PROPERTY = "cassandra.tracingfrequency";
  public static final String TRACING_PROPERTY_DEFAULT = "false";
  public static final String TRACING_FREQUENCY_PROPERTY_DEFAULT = "1000";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final AtomicInteger TRACE_COUNT = new AtomicInteger(0);
  private static boolean trace = false;
  private static int traceFrequency;
  
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
      if (session != null) {
        return;
      }
      try {
        trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));
        traceFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_PROPERTY,
            TRACING_FREQUENCY_PROPERTY_DEFAULT
        ));
        String driverConfigPath = getProperties().getProperty(DRIVER_CONFIG_PROPERTY);
        if (driverConfigPath == null) {
          throw new IllegalArgumentException("Missing required driver config file path set via: '"
              + DRIVER_CONFIG_PROPERTY + "'");
        }
        File driverConfigFile = new File(driverConfigPath);
        DriverConfigLoader configLoader = DriverConfigLoader.fromFile(driverConfigFile);
        DriverConfig driverConfig = configLoader.getInitialConfig();
        Function<String, DriverExecutionProfile> profileSupplier = (String profile) -> profile == null
            ? driverConfig.getDefaultProfile()
            : driverConfig.getProfile(profile);
        readProfile = profileSupplier.apply(getProperties().getProperty(DRIVER_PROFILE_READ_PROPERTY));
        scanProfile = profileSupplier.apply(getProperties().getProperty(DRIVER_PROFILE_SCAN_PROPERTY));
        insertProfile = profileSupplier.apply(getProperties().getProperty(DRIVER_PROFILE_INSERT_PROPERTY));
        updateProfile = profileSupplier.apply(getProperties().getProperty(DRIVER_PROFILE_UPDATE_PROPERTY));
        deleteProfile = profileSupplier.apply(getProperties().getProperty(DRIVER_PROFILE_DELETE_PROPERTY));
        session = CqlSession.builder()
            .withConfigLoader(DriverConfigLoader.fromFile(driverConfigFile))
            .build();
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
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);
      // Prepare statement on demand
      if (stmt == null) {
        Select select;
        if (fields == null) {
          select = QueryBuilder.selectFrom(table).all();
        } else {
          select = QueryBuilder.selectFrom(table).columns(fields);
        }
        SimpleStatement simpleStatement = select.where(Relation.column(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker()))
            .limit(1)
            .build()
            .setConsistencyLevel(readConsistencyLevel)
            .setTracing(trace)
            .setExecutionProfile(readProfile);
        stmt = session.prepare(simpleStatement);
        PreparedStatement prevStmt = (fields == null) ?
                                     readAllStmt.getAndSet(stmt) :
                                     readStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);
      ResultSet rs = session.execute(stmt.bind(key));
      // Should be only 1 row
      Row row = rs.one();
      if (row == null) {
        return Status.NOT_FOUND;
      }
      ColumnDefinitions cd = row.getColumnDefinitions();
      for (final ColumnDefinition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName().toString());
        if (val != null) {
          result.put(def.getName().toString(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName().toString(), null);
        }
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
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
      PreparedStatement stmt = (fields == null) ? scanAllStmt.get() : scanStmts.get(fields);
      // Prepare statement on demand
      if (stmt == null) {
        Select select;
        if (fields == null) {
          select = QueryBuilder.selectFrom(table).all();
        } else {
          select = QueryBuilder.selectFrom(table).columns(fields);
        }
        SimpleStatement simpleStatement = select.where(Relation.token(YCSB_KEY)
                .isGreaterThanOrEqualTo(QueryBuilder.function(
                    "\"token\"",
                    QueryBuilder.bindMarker()
                ))
            ).limit(QueryBuilder.bindMarker())
            .build()
            .setConsistencyLevel(readConsistencyLevel)
            .setTracing(trace)
            .setExecutionProfile(scanProfile);
        stmt = session.prepare(simpleStatement);
        final PreparedStatement prevStmt = (fields == null) ?
                                     scanAllStmt.getAndSet(stmt) :
                                     scanStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      logger.debug(stmt.getQuery());
      logger.debug("startKey = {}, record count = {}", startkey, recordcount);
      ResultSet rs = session.execute(stmt.bind(startkey, recordcount));
      if (trace) {
        int count = TRACE_COUNT.getAndIncrement();
        if (count % traceFrequency == 0) {
          ExecutionInfo executionInfo = rs.getExecutionInfo();
          UUID tracingId = executionInfo.getTracingId();
          QueryTrace qtrace = executionInfo.getQueryTrace();
          logger.info(
              "[{}] '{}' to {} took {}μs",
              tracingId,
              qtrace.getRequestType(),
              qtrace.getCoordinatorAddress(),
              qtrace.getDurationMicros()
          );
          for (final TraceEvent event : qtrace.getEvents()) {
            logger.info(
                " - [{}] {} :: {}",
                event.getSourceElapsedMicros(),
                event.getThreadName(),
                event.getActivity()
            );
          }
        }
      }
      HashMap<String, ByteIterator> tuple;
      Row row;
      while ((row = rs.one()) != null) {
        tuple = new HashMap<>();
        ColumnDefinitions cd = row.getColumnDefinitions();
        for (ColumnDefinition def : cd) {
          String name = def.getName().toString();
          ByteBuffer val = row.getBytesUnsafe(name);
          if (val != null) {
            tuple.put(name, new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(name, null);
          }
        }
        result.add(tuple);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(
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
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        UpdateStart updateStmt = QueryBuilder.update(table);
        // Add fields
        List<Assignment> assignments = fields.stream()
            .map((final String field) -> Assignment.setColumn(field, QueryBuilder.bindMarker()))
            .collect(Collectors.toList());
        // Add keying
        Update update = updateStmt.set(assignments)
            .where(Relation.column(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker()));
        SimpleStatement simpleStatement = update.build()
            .setConsistencyLevel(writeConsistencyLevel)
            .setTracing(trace)
            .setExecutionProfile(updateProfile);
        stmt = session.prepare(simpleStatement);
        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }
      // Add fields
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 1; i++) {
        boundStmt = boundStmt.setString(
            i,
            values.get(
                vars.get(i)
                    .getName()
                    .toString()
            ).toString()
        );
      }
      // Add key
      boundStmt = boundStmt.setString(vars.size() - 1, key);
      session.execute(boundStmt);
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
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
      PreparedStatement stmt = insertStmts.get(fields);
      // Prepare statement on demand
      if (stmt == null) {
        InsertInto insertStmt = QueryBuilder.insertInto(table);
        // Add key
        RegularInsert regularInsert = insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker())
            .values(fields.stream().collect(Collectors.toMap(
                Function.identity(),
                (String ignored) -> QueryBuilder.bindMarker()
            )));
        SimpleStatement simpleStatement = regularInsert.build()
            .setConsistencyLevel(writeConsistencyLevel)
            .setTracing(trace)
            .setExecutionProfile(insertProfile);
        stmt = session.prepare(simpleStatement);
        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }
      // Add key
      BoundStatement boundStmt = stmt.bind().setString(0, key);
      // Add fields
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      for (int i = 1; i < vars.size(); i++) {
        boundStmt = boundStmt.setString(
            i,
            values.get(
                vars.get(i)
                    .getName()
                    .toString()
            ).toString()
        );
      }
      session.execute(boundStmt);
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
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
      PreparedStatement stmt = deleteStmt.get();
      // Prepare statement on demand
      if (stmt == null) {
        Delete delete = QueryBuilder.deleteFrom(table)
            .where(Relation.column(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker()));
        stmt = session.prepare(delete.build()
            .setConsistencyLevel(writeConsistencyLevel)
            .setTracing(trace)
            .setExecutionProfile(deleteProfile)
        );
        PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);
      session.execute(stmt.bind(key));
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }
    return Status.ERROR;
  }

}
