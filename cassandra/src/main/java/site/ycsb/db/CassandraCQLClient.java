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
 * Updated by EngineersBox (Jack Kilrain) on 17/07/2024
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * Cassandra CQL client for versions >= 2.1.
 *
 * See {@code cassandra/README.md} for details.
 *
 * @author cmatser
 * @author EngineersBox (Jack Kilrain)
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
  public static final String DRIVER_PROFILE_SCAN_PROPERTY = "cassandra.driverprofile.scan";
  public static final String DRIVER_PROFILE_INSERT_PROPERTY = "cassandra.driverprofile.insert";
  public static final String DRIVER_PROFILE_UPDATE_PROPERTY = "cassandra.driverprofile.update";
  public static final String DRIVER_PROFILE_DELETE_PROPERTY = "cassandra.driverprofile.delete";
  public static final String TRACING_READ_PROPERTY = "cassandra.tracing.read";
  public static final String TRACING_SCAN_PROPERTY = "cassandra.tracing.scan";
  public static final String TRACING_INSERT_PROPERTY = "cassandra.tracing.insert";
  public static final String TRACING_UPDATE_PROPERTY = "cassandra.tracing.update";
  public static final String TRACING_DELETE_PROPERTY = "cassandra.tracing.delete";
  public static final String TRACING_FREQUENCY_READ_PROPERTY = "cassandra.tracingfrequency.read";
  public static final String TRACING_FREQUENCY_SCAN_PROPERTY = "cassandra.tracingfrequency.scan";
  public static final String TRACING_FREQUENCY_INSERT_PROPERTY = "cassandra.tracingfrequency.insert";
  public static final String TRACING_FREQUENCY_UPDATE_PROPERTY = "cassandra.tracingfrequency.update";
  public static final String TRACING_FREQUENCY_DELETE_PROPERTY = "cassandra.tracingfrequency.delete";
  public static final String TRACING_PROPERTY_DEFAULT = "false";
  public static final String TRACING_FREQUENCY_PROPERTY_DEFAULT = "1000";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final AtomicLong READ_TRACE_COUNT = new AtomicLong(0);
  private static final AtomicLong SCAN_TRACE_COUNT = new AtomicLong(0);
  private static final AtomicLong INSERT_TRACE_COUNT = new AtomicLong(0);
  private static final AtomicLong UPDATE_TRACE_COUNT = new AtomicLong(0);
  private static final AtomicLong DELETE_TRACE_COUNT = new AtomicLong(0);
  private static boolean traceRead = false;
  private static boolean traceScan = false;
  private static boolean traceInsert = false;
  private static boolean traceUpdate = false;
  private static boolean traceDelete = false;
  private static int traceReadFrequency = 1000;
  private static int traceScanFrequency = 1000;
  private static int traceInsertFrequency = 1000;
  private static int traceUpdateFrequency = 1000;
  private static int traceDeleteFrequency = 1000;


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
        traceRead = Boolean.parseBoolean(getProperties().getProperty(
            TRACING_READ_PROPERTY,
            TRACING_PROPERTY_DEFAULT
        ));
        traceReadFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_READ_PROPERTY,
            TRACING_FREQUENCY_PROPERTY_DEFAULT
        ));
        traceScan = Boolean.parseBoolean(getProperties().getProperty(
            TRACING_SCAN_PROPERTY,
            TRACING_PROPERTY_DEFAULT
        ));
        traceScanFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_SCAN_PROPERTY,
            TRACING_FREQUENCY_PROPERTY_DEFAULT
        ));
        traceInsert = Boolean.parseBoolean(getProperties().getProperty(
            TRACING_INSERT_PROPERTY,
            TRACING_PROPERTY_DEFAULT
        ));
        traceInsertFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_INSERT_PROPERTY,
            TRACING_FREQUENCY_PROPERTY_DEFAULT
        ));
        traceUpdate = Boolean.parseBoolean(getProperties().getProperty(
            TRACING_UPDATE_PROPERTY,
            TRACING_PROPERTY_DEFAULT
        ));
        traceUpdateFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_UPDATE_PROPERTY,
            TRACING_FREQUENCY_PROPERTY_DEFAULT
        ));
        traceDelete = Boolean.parseBoolean(getProperties().getProperty(
            TRACING_DELETE_PROPERTY,
            TRACING_PROPERTY_DEFAULT
        ));
        traceDeleteFrequency = Integer.parseInt(getProperties().getProperty(
            TRACING_FREQUENCY_DELETE_PROPERTY,
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
        READ_TRACE_COUNT.set(0);
        SCAN_TRACE_COUNT.set(0);
        INSERT_TRACE_COUNT.set(0);
        UPDATE_TRACE_COUNT.set(0);
        DELETE_TRACE_COUNT.set(0);
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
   * Logs the trace events of a query submitted with tracing enabled.
   *
   * @param rs {@code ResultSet} from a {@code session.execute(...)} invocation.
   */
  private void logTraceOutput(final ResultSet rs) {
    final ExecutionInfo executionInfo = rs.getExecutionInfo();
    final UUID tracingId = executionInfo.getTracingId();
    final QueryTrace qtrace = executionInfo.getQueryTrace();
    logger.info(
        "[{}] '{}' to {} took {}μs",
        tracingId,
        qtrace.getRequestType(),
        qtrace.getCoordinatorAddress(),
        qtrace.getDurationMicros()
    );
    int eventIndex = 0;
    for (final TraceEvent event : qtrace.getEvents()) {
      logger.info(
          " {} - [{}μs] {} :: {}",
          eventIndex++,
          event.getSourceElapsedMicros(),
          event.getThreadName(),
          event.getActivity()
      );
    }
  }

  private boolean shouldTraceQuery(final boolean flag, final AtomicLong counter, final int frequency) {
    return flag && counter.get() % frequency == 0;
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
    final boolean shouldTrace = shouldTraceQuery(traceRead, READ_TRACE_COUNT, traceReadFrequency);
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
            .setTracing(shouldTrace)
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
      if (shouldTrace) {
        logTraceOutput(rs);
      }
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
    final boolean shouldTrace = shouldTraceQuery(traceScan, SCAN_TRACE_COUNT, traceScanFrequency);
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
            .setTracing(shouldTrace)
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
      if (shouldTrace) {
        logTraceOutput(rs);
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
    final boolean shouldTrace = shouldTraceQuery(traceUpdate, UPDATE_TRACE_COUNT, traceUpdateFrequency);
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
            .setTracing(shouldTrace)
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
      final ResultSet rs = session.execute(boundStmt);
      if (shouldTrace) {
        logTraceOutput(rs);
      }
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
    final boolean shouldTrace = shouldTraceQuery(traceInsert, INSERT_TRACE_COUNT, traceInsertFrequency);
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
            .setTracing(shouldTrace)
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
      final ResultSet rs = session.execute(boundStmt);
      if (shouldTrace) {
        logTraceOutput(rs);
      }
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
    final boolean shouldTrace = shouldTraceQuery(traceDelete, DELETE_TRACE_COUNT, traceDeleteFrequency);
    try {
      PreparedStatement stmt = deleteStmt.get();
      // Prepare statement on demand
      if (stmt == null) {
        Delete delete = QueryBuilder.deleteFrom(table)
            .where(Relation.column(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker()));
        stmt = session.prepare(delete.build()
            .setConsistencyLevel(writeConsistencyLevel)
            .setTracing(shouldTrace)
            .setExecutionProfile(deleteProfile)
        );
        PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }
      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);
      final ResultSet rs = session.execute(stmt.bind(key));
      if (shouldTrace) {
        logTraceOutput(rs);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }
    return Status.ERROR;
  }

}
