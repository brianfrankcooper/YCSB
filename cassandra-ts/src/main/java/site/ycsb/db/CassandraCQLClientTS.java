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
import site.ycsb.workloads.TimeSeriesWorkload;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


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

  // global debug property loading
  private static final String DEBUG_PROPERTY = "debug";
  private static final String DEBUG_PROPERTY_DEFAULT = "false";

  // class-specific debug property loading
  private static final String CASSANDRA_DEBUG_PROPERTY = "cassandra.debug";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;
  private static boolean missingreadsdebug = false;

  private static boolean trace = false;

  private Integer tagCount;

  private Gson gson;
  private Type jSonToMapType;
  
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // Call init() from TimeseriesDB to ensure correct 
    // workload parsing
    super.init();
    
    // One Gson instance per-thread to avoid unnecessary instantiations
    gson = new Gson();

    // For use with Gson to convert JSON to a Map<String, String>
    jSonToMapType = new TypeToken<Map<String, String>>(){}.getType();

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
            Boolean.parseBoolean(
                getProperties()
                  .getProperty(
                    DEBUG_PROPERTY,
                    getProperties()
                      .getProperty(CASSANDRA_DEBUG_PROPERTY, DEBUG_PROPERTY_DEFAULT)));
        missingreadsdebug = Boolean.parseBoolean(getProperties().getProperty("missingreadsdebug", "false"));
        if (debug) {
          logger.info("Workload Properties: \n" + getProperties());
        }
        trace = Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        tagCount = Integer.parseInt(getProperties().getProperty(
              TimeSeriesWorkload.TAG_COUNT_PROPERTY,
              TimeSeriesWorkload.TAG_COUNT_PROPERTY_DEFAULT));

        if (tagCount == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClientTS",
              TimeSeriesWorkload.TAG_COUNT_PROPERTY));

        }

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClientTS",
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
  public Status read(String metric, long timestamp, Map<String,
                     List<String>> tags, Map<String, ByteIterator> result) {
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
        logger.info("[READ]   metric: " + metric + ", tags: " + tagsQueryAsJson + ", timestamp: " + timestamp + "(" + new Date(timestamp * 1000) + ")");
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
        if (missingreadsdebug) {
          logger.info("[READ]   metric: " + metric + ", tags: " + tagsQueryAsJson + ", timestamp: " + timestamp + "(" + new Date(timestamp * 1000) + ")");
          logger.info("[READ][NOT FOUND]");
          logger.info("[READ][Querying all records for the series with this Key/Tag combo)]");
          Select.Builder allForSeriesSelectBuilder = QueryBuilder.select();
          ((Select.Selection) allForSeriesSelectBuilder).column("metric");
          ((Select.Selection) allForSeriesSelectBuilder).column("tags");
          ((Select.Selection) allForSeriesSelectBuilder).column("valuetime");
          ((Select.Selection) allForSeriesSelectBuilder).column("value");
          PreparedStatement allForSeriesStmt = session.prepare(allForSeriesSelectBuilder.from(table)
                                                      .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
                                                      .and(QueryBuilder.eq("tags", QueryBuilder.bindMarker())));
          allForSeriesStmt.setConsistencyLevel(readConsistencyLevel);
          BoundStatement allForSeriesBoundStmt = allForSeriesStmt.bind().setString(YCSB_KEY, metric);
          allForSeriesBoundStmt.setString("tags", tagsQueryAsJson);
          ResultSet allForSeriesRs = session.execute(allForSeriesBoundStmt);
          if (rs.isExhausted()) {
            logger.info("[READ]\t[No other records for this Key/Tag combo found!]\n\n");
          } else {
            for(Row row : allForSeriesRs) {
              logger.info("[READ]\t[Row][metric: " + row.getString("metric") + ", tags: " + row.getString("tags") + ", timestamp: " + row.getTimestamp("valuetime").getTime() + "(" + row.getTimestamp("valuetime") + "), value: " + row.getDouble("value") + "]");
            }
            logger.info("[READ]\t[END of records for this Key/Tag combo]\n\n");
          }
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
      if (missingreadsdebug) {
        logger.info("[READ]   metric: " + metric + ", tags: " + tagsQueryAsJson + ", timestamp: " + timestamp + "(" + new Date(timestamp * 1000) + ")");
        logger.info("[READ][result] value: " + resultValue + "\n");
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
   * @param metric                The name of the metric
   * @param startTs               The timestamp of the first record to read.
   * @param endTs                 The timestamp of the last record to read.
   * @param tags                  The actual tags that were want to receive (can be empty).
   * @param downsamplingFunction  The aggregation operation for downsampling.
   * @param timeValue             value for timeUnit for aggregation.
   * @param timeUnit              timeUnit for aggregation.
   * @param groupByFunction       The aggregation function for group by.
   * @param groupByTags           The tag(s) that should be grouped by.
   * @return A {@link Status} detailing the outcome of the scan operation.
   */
  @Override
  public Status scan(String metric, long startTs, long endTs, Map<String, List<String>> tags,
                     AggregationOperation downsamplingFunction, int downsamplingWindowLength,
                     TimeUnit downsamplingWindowUnit, AggregationOperation groupByFunction,
                     Set<String> groupByTags, Vector<HashMap<String, ByteIterator>> result) {
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
      String tagsQueryAsJson = gson.toJson(tagsMap);
      if (debug) {
        logger.info("[SCAN] metric: " + metric + ", tags: " + tagsQueryAsJson + ", startTs: " + startTs + ", endTs: " + endTs + ", downsamplingFunction: " + downsamplingFunction + ", downsamplingWindowLength: " + downsamplingWindowLength + ", downsamplingWindowUnit: " + downsamplingWindowUnit + ", groupByFunction: " + groupByFunction + ", groupByTags: " + groupByTags);
      }


      Set<String> queryFields = new HashSet();
      queryFields.add("valuetime");
      if (groupByFunction.toString() != "NONE") {
        queryFields.add("value-groupby");
      } else {
        queryFields.add("value");
      }
      

      PreparedStatement stmt = scanStmts.get(queryFields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        selectBuilder = QueryBuilder.select();
        ((Select.Selection) selectBuilder).column("valuetime");
        ((Select.Selection) selectBuilder).column("value");
        ((Select.Selection) selectBuilder).column("tags");

        Select.Where scanStmt = selectBuilder.from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        scanStmt.and(QueryBuilder.gte("valuetime", QueryBuilder.bindMarker("startTs")));
        scanStmt.and(QueryBuilder.lte("valuetime", QueryBuilder.bindMarker("endTs")));
        if (tagCount == tagsMap.size()) { // We have a fully-qualified tag query
          scanStmt.and(QueryBuilder.eq("tags", QueryBuilder.bindMarker()));
        } else {
          // Without a fully-qualified tag query, we will need to allow filtering
          // for Cassandra to accept the query, because the we cannot query
          // the tags column without a fully-specified tags list, and the tags
          // column is a clustering column and part of the primary key.
          // (NOTE: this has unknown performance implications!)
          scanStmt.allowFiltering(); 
        }

        stmt = session.prepare(scanStmt);

        //stmt = session.prepare(selectBuilder.from(table)
                               //.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
                               //.and(QueryBuilder.eq("tags", QueryBuilder.bindMarker()))
                               //.and(QueryBuilder.gte("valuetime", QueryBuilder.bindMarker("startts")))
                               //.and(QueryBuilder.lte("valuetime", QueryBuilder.bindMarker("endTs"))));

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

      // Add tags (if available)
      if (tagCount == tagsMap.size()) {
        boundStmt.setString("tags", tagsQueryAsJson);
      }       

      ResultSet rs = session.execute(boundStmt);

      if (rs.isExhausted()) {
        if (debug) {
          logger.info("[SCAN][NOT FOUND]\n\n");
        }
        return Status.NOT_FOUND;
      }

      // no client-side GROUP BY aggregation OR client-side downsampling
      if (groupByFunction.toString() == "NONE" && downsamplingFunction.toString() == "NONE") {
        Stream<Row> resultSetStream = StreamSupport.stream(rs.spliterator(), false);

        // First filter by tag query if there was any
        // This is uses a non-terminating stream filtering operation,
        // so the stream is still open for the collection afterwards.
        resultSetStream = filterResultSetStreamByTags(resultSetStream, tagsMap);
        // Output results
        resultSetStream.forEach( row -> {
          Date resultTimestamp = row.getTimestamp("valuetime");
          Double resultValue = row.getDouble("value");
          if (debug) {
            logger.info("[SCAN][result]" + rowToString(row));
          }
        });
      } else if (downsamplingFunction.toString() != "NONE" && groupByFunction.toString() == "NONE") {
        // client-side downsampling required, but no client-side GROUP BY
        if (debug ) {
          logger.info("[SCAN][client-side downsampling] downsamplingFunction: " + downsamplingFunction.toString() + ", downsamplingWindowLength: " + downsamplingWindowLength + ", downsamplingWindowUnit: " + downsamplingWindowUnit);
        }
        Stream<Row> resultSetStream = StreamSupport.stream(rs.spliterator(), false);

        // First filter by tag query if there was any
        // This is uses a non-terminating stream filtering operation,
        // so the stream is still open for the collection afterwards.
        resultSetStream = filterResultSetStreamByTags(resultSetStream, tagsMap);

        Map<String, ? extends Map<String, ? extends Number>> downsamplingResults = new LinkedHashMap();
        downsamplingResults = resultSetStream.collect(
          Collectors.groupingBy(
            row -> row.getString("tags"),
            LinkedHashMap::new,
            Collectors.groupingBy(
              row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
              LinkedHashMap::new,
              aggregateRows(downsamplingFunction, row -> row.getDouble("value")))));
        //switch (downsamplingFunction.toString()) {
          //case "SUM":
            //downsamplingResults = resultSetStream.collect(
              //Collectors.groupingBy(row -> row.getString("tags"),
                //LinkedHashMap::new,
                //Collectors.groupingBy(row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
                  //LinkedHashMap::new, Collectors.summingDouble(row -> row.getDouble("value"))))
            //);
            //break;
          //case "AVERAGE":
            //downsamplingResults = resultSetStream.collect(
              //Collectors.groupingBy(row -> row.getString("tags"),
                //LinkedHashMap::new,
                //Collectors.groupingBy(row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
                  //LinkedHashMap::new, Collectors.averagingDouble(row -> row.getDouble("value"))))
            //);
            //break;
          //case "MAX":
            //downsamplingResults = resultSetStream.collect(
              //Collectors.groupingBy(row -> row.getString("tags"),
                //LinkedHashMap::new,
                //Collectors.groupingBy(row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
                  //LinkedHashMap::new,
                  //Collectors.collectingAndThen(Collectors.maxBy(Comparator.comparingDouble(row -> row.getDouble("value"))),
                    //maxRow -> maxRow.isPresent() ? maxRow.get().getDouble("value") : 0)))
            //);
            //break;
          //case "MIN":
            //downsamplingResults = resultSetStream.collect(
              //Collectors.groupingBy(row -> row.getString("tags"),
                //LinkedHashMap::new,
                //Collectors.groupingBy(row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
                  //LinkedHashMap::new,
                  //Collectors.collectingAndThen(Collectors.minBy(Comparator.comparingDouble(row -> row.getDouble("value"))),
                    //minRow -> minRow.isPresent() ? minRow.get().getDouble("value") : 0)))
            //);
            //break;
          //case "COUNT":
            //downsamplingResults = resultSetStream.collect(
              //Collectors.groupingBy(row -> row.getString("tags"),
                //LinkedHashMap::new,
                //Collectors.groupingBy(row -> groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit),
                  //LinkedHashMap::new, Collectors.counting()))
            //);
            //break;
          //default:
            //throw new IllegalArgumentException("Unsupported downsamplingFunction: " + downsamplingFunction.toString());
        //}
        if (debug) {
          logger.info("[SCAN][Downsampling Results][downsamplingFunction = " + downsamplingFunction.toString() + "][downsamplingWindowLength = " + downsamplingWindowLength + "][downsamplingWindowUnit = " + downsamplingWindowUnit + "]");
          logMap(downsamplingResults);
          //downsamplingResults.forEach((timeBucket, value) -> {
            //logger.info(timeBucket + "(" + new Date(new Long(timeBucket)) + ") --> value = " + value);
          //});
          logger.info("[SCAN][/Downsampling Result]");
        }// END client-side downsampling
      } else if (downsamplingFunction.toString() == "NONE" && groupByFunction.toString() != "NONE") {
        // client-side GROUP BY aggregation required, but NO client-side downsampling
        Stream<Row> resultSetStream = StreamSupport.stream(rs.spliterator(), false);

        // First filter by tag query if there was any
        resultSetStream = filterResultSetStreamByTags(resultSetStream, tagsMap);

        // When group by is enabled we are given one or more tags to group by
        if (debug) { logger.info("[SCAN][grouping by tag(s)] tags: " + groupByTags); }
        //LinkedHashMap<String, LinkedHashMap<String, ResultContainer>> groupByResults = new LinkedHashMap();
        Map<String, ? extends Map<String, ? extends Number>> groupByResults = new LinkedHashMap();
        groupByResults = resultSetStream.collect(
          Collectors.groupingBy(
            row -> groupRowByGroupByTags(row, groupByTags),
            LinkedHashMap::new,
            Collectors.groupingBy(
              rowGroupedByTag -> {
                return groupRowByTimestamp(rowGroupedByTag);
              },
              LinkedHashMap::new,
              aggregateRows(groupByFunction, row -> row.getDouble("value"))))
        );
        if (debug) {
          logger.info("[SCAN][GroupBy Results][groupByTags = " + groupByTags + "][groupByFunction = " + groupByFunction.toString() + "]");
          logMap(groupByResults);
          //groupByResults.forEach((tagsGroup, resultByTimestamp) -> {
            //logger.info(tagsGroup + " -->");
            //resultByTimestamp.forEach((timestamp, result) -> {
              //logger.info("\t" + timestamp+ " --> result = " + result);
            //});
          //});
          logger.info("[SCAN][/GroupBy Result]");
        }



        //switch (groupByFunction.toString()) {
          //case "SUM":
            //groupByResults = resultSetStream.collect(
              //Collectors.collectingAndThen(
                //Collectors.groupingBy(ungroupedRow -> groupRowByGroupByTags(ungroupedRow, groupByTags),
                  //LinkedHashMap::new,
                  //Collectors.collectingAndThen(
                    //Collectors.groupingBy((Row rowGroupedByTag) -> groupRowByTimestamp(rowGroupedByTag),
                      //LinkedHashMap::new,
                      //Collectors.<Row>toList()),
                    //(LinkedHashMap<String, List<Row>> mapGroupedByTagAndTimestamp) -> {
                      ////if (debug) {
                        ////logMap(mapGroupedByTagAndTimestamp);
                      ////}
                      //return mapGroupedByTagAndTimestamp.entrySet().stream().collect(
                          //Collectors.toMap((Map.Entry entry) -> (String)entry.getKey(),
                            //(Map.Entry entry) -> {
                              //Map<String, String> resultTags = groupedTagsForRow(((List<Row>)entry.getValue()).get(0), groupByTags);
                              //tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                              //return new ResultContainer(metric,
                                  //gson.toJson(resultTags),
                                  //(String)entry.getKey(),
                                  //((List<Row>)entry.getValue()).stream().mapToDouble(row -> row.getDouble("value")).sum());
                            //},
                            //(u, v) -> {
                              //throw new IllegalStateException(String.format("Duplicate key %s", u));
                            //},
                            //LinkedHashMap::new));
                    //})
                  ////Collectors.groupingBy(rowGroupedByTag -> groupRowByTimestamp(rowGroupedByTag),
                    ////LinkedHashMap::new,
                    ////Collectors.reducing(new ResultContainer(metric, tagsQueryAsJson, 0, 0.0),
                      ////rowGroupedByTagAndTimestamp -> {
                        ////Map<String, String> resultTags = groupedTagsForRow(rowGroupedByTagAndTimestamp, groupByTags);
                        ////tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                        ////ResultContainer res = new ResultContainer(metric,
                          ////gson.toJson(resultTags),
                          ////rowGroupedByTagAndTimestamp.getTimestamp("valuetime").getTime(),
                          ////rowGroupedByTagAndTimestamp.getDouble("value"));
                        ////if (debug) {
                          ////logger.info("\t[row by TS & Tag]: " + res);
                        ////}
                        ////return res;
                      ////}, (a,b) -> {
                        ////b.value = a.value + b.value;
                        ////return b;
                      ////}
                    ////)
                  ////)
                //),
                //mapGroupedByTag -> {
                  ////if (debug) {
                    ////logGroupByResults(mapGroupedByTag, groupByTags, groupByFunction);
                  ////}
                  //return mapGroupedByTag;
                //}
              //)
            //);
            //break;
          //case "AVERAGE":
            //groupByResults = resultSetStream.collect(
              //Collectors.groupingBy(ungroupedRow -> groupRowByGroupByTags(ungroupedRow, groupByTags),
                //LinkedHashMap::new,
                //Collectors.groupingBy(rowGroupedByTag -> groupRowByTimestamp(rowGroupedByTag),
                  //LinkedHashMap::new,
                  //Collector.of(
                    //() -> new ArrayList<ResultContainer>(),
                    //(collector, rowGroupedByTagAndTimestamp) -> {
                      //Map<String, String> resultTags = groupedTagsForRow(rowGroupedByTagAndTimestamp, groupByTags);
                      //tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                      //collector.add(new ResultContainer(metric, gson.toJson(resultTags), rowGroupedByTagAndTimestamp.getTimestamp("valuetime").getTime(), rowGroupedByTagAndTimestamp.getDouble("value")));
                    //},
                    //(collector1, collector2) -> {
                      //collector1.addAll(collector2);
                      //return collector1;
                    //},
                    //collector -> {
                      //if (collector.isEmpty()) {
                        //return new ResultContainer(metric, tagsQueryAsJson, "------", 0.0);
                      //} else {
                        //ResultContainer rc = collector.get(0);
                        //double avg = collector.stream().mapToDouble(resCont -> resCont.value).average().orElse(0);
                        //rc.value = avg;
                        //return rc;
                      //}
                    //}
                  //)))
            //);
            //break;
          //case "MAX":
            //groupByResults = resultSetStream.collect(
              //Collectors.groupingBy(ungroupedRow -> groupRowByGroupByTags(ungroupedRow, groupByTags),
                //LinkedHashMap::new,
                //Collectors.groupingBy(rowGroupedByTag -> groupRowByTimestamp(rowGroupedByTag),
                  //LinkedHashMap::new,
                  //Collectors.reducing(new ResultContainer(metric, tagsQueryAsJson, "------", 0.0),
                    //rowGroupedByTagAndTimestamp -> {
                      //Map<String, String> resultTags = groupedTagsForRow(rowGroupedByTagAndTimestamp, groupByTags);
                      //tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                      //return new ResultContainer(metric, gson.toJson(resultTags), rowGroupedByTagAndTimestamp.getTimestamp("valuetime").getTime(), rowGroupedByTagAndTimestamp.getDouble("value"));
                    //}, (a,b) -> {
                      //return b.value > a.value ? b : a;
                    //})))
            //);
            //break;
          //case "MIN":
            //groupByResults = resultSetStream.collect(
              //Collectors.groupingBy(ungroupedRow -> groupRowByGroupByTags(ungroupedRow, groupByTags),
                //LinkedHashMap::new,
                //Collectors.groupingBy(rowGroupedByTag -> groupRowByTimestamp(rowGroupedByTag),
                  //LinkedHashMap::new,
                  //Collectors.reducing(new ResultContainer(metric, tagsQueryAsJson, "------", 0.0),
                    //rowGroupedByTagAndTimestamp -> {
                      //Map<String, String> resultTags = groupedTagsForRow(rowGroupedByTagAndTimestamp, groupByTags);
                      //tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                      //return new ResultContainer(metric, gson.toJson(resultTags), rowGroupedByTagAndTimestamp.getTimestamp("valuetime").getTime(), rowGroupedByTagAndTimestamp.getDouble("value"));
                    //}, (a,b) -> {
                      //return b.value < a.value ? b : a;
                    //})))
            //);
            //break;
          //case "COUNT":
            //groupByResults = resultSetStream.collect(
              //Collectors.groupingBy(ungroupedRow -> groupRowByGroupByTags(ungroupedRow, groupByTags),
                //LinkedHashMap::new,
                //Collectors.groupingBy(rowGroupedByTag -> groupRowByTimestamp(rowGroupedByTag),
                  //LinkedHashMap::new,
                  //Collectors.reducing(new ResultContainer(metric, tagsQueryAsJson, "------", 0.0),
                    //rowGroupedByTagAndTimestamp -> {
                      //Map<String, String> resultTags = groupedTagsForRow(rowGroupedByTagAndTimestamp, groupByTags);
                      //tagsMap.forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
                      //// We just return a ResultContainer with the value set to 1.0, cos we will sum them later to get the count
                      //return new ResultContainer(metric, gson.toJson(resultTags), rowGroupedByTagAndTimestamp.getTimestamp("valuetime").getTime(), 1.0);
                    //}, (a,b) -> {
                      //b.value = a.value + b.value;
                      //return b;
                    //})))
            //);
            //break;
          //default:
            //throw new IllegalArgumentException("Unsupported groupByFunction: " + groupByFunction.toString());
        //}

      } else {
        // BOTH client-side downsampling and client-side GROUP BY are required!
        Stream<Row> resultSetStream = StreamSupport.stream(rs.spliterator(), false);

        // First filter by tag query if there was any
        // This is uses a non-terminating stream filtering operation,
        // so the stream is still open for the collection afterwards.
        resultSetStream = filterResultSetStreamByTags(resultSetStream, tagsMap);

        // When group by is enabled we are given one or more tags to group by
        if (debug) { logger.info("[SCAN][GROUP BY AND DOWNSAMPLE][groupByTags = " + groupByTags + "][groupByFunction = " + groupByFunction.toString() + "][downsamplingFunction = " + downsamplingFunction.toString() + "][downsamplingWindowLength = " + downsamplingWindowLength + "][downsamplingWindowUnit = " + downsamplingWindowUnit + "] "); }
        Map<String, ? extends Map<String, ? extends Number>> groupByAndDownsamplingResults = new LinkedHashMap();
        groupByAndDownsamplingResults = resultSetStream.collect(
          Collectors.groupingBy(
            row -> groupRowByGroupByTags(row, groupByTags),
            LinkedHashMap::new,
            Collectors.groupingBy(
              rowGroupedByTag -> groupRowByDownsampledTimestamp(rowGroupedByTag,
                                   downsamplingWindowLength,
                                   downsamplingWindowUnit),
              LinkedHashMap::new,
              Collectors.collectingAndThen(
                Collectors.groupingBy(
                  rowGroupedByDownsampledTimestamp -> {
                    return groupRowByTimestamp(rowGroupedByDownsampledTimestamp);
                  },
                  LinkedHashMap::new,
                  aggregateRows(groupByFunction, row -> row.getDouble("value"))),
                mapRowsGroupedByTimestamp -> {
                  return mapRowsGroupedByTimestamp.entrySet().stream().collect(
                      aggregateRows(
                          downsamplingFunction,
                          (Map.Entry entry) -> ((Number) entry.getValue()).doubleValue()));
                })))
        );
        if (debug) {
          logger.info("[SCAN][GROUP BY AND DOWNSAMPLE **RESULTS**][groupByTags = " + groupByTags + "][groupByFunction = " + groupByFunction.toString() + "][downsamplingFunction = " + downsamplingFunction.toString() + "][downsamplingWindowLength = " + downsamplingWindowLength + "][downsamplingWindowUnit = " + downsamplingWindowUnit + "] ");
          logMap(groupByAndDownsamplingResults);
          //groupByAndDownsamplingResults.forEach((tagsGroup, resultsByDownsampledTimestamp) -> {
            //logger.info(tagsGroup + " -->");
            //resultsByDownsampledTimestamp.forEach((downsampledTimestamp, result) -> {
              //logger.info("\t\t" + downsampledTimestamp + " --> result = " + result);
            //});
          //});
          logger.info("[SCAN][/GROUP BY AND DOWNSAMPLE **RESULTS**]");
        }
        
        //Map<String, Map<String, List<Row>>> rowsByTagsandTimestamp = new LinkedHashMap();
        //Type jSonToMapType = new TypeToken<Map<String, String>>(){}.getType();
        //rowsByTagsandTimestamp = resultSetStream.collect(
          //Collectors.groupingBy(row -> groupRowByGroupByTags(row, groupByTags),
            //LinkedHashMap::new,
            //Collectors.groupingBy(row -> groupRowByTimestamp(row),
              //LinkedHashMap::new,
              //Collectors.toList()))
        //);
        //if (debug) {
          //logger.info("[SCAN][Rows grouped by tag AND timestamp][groupByTags = " + groupByTags + "]");
          //rowsByTagsandTimestamp.forEach((tagsGroup, rowsByTimestamp) -> {
            //logger.info(tagsGroup + " -->");
            //rowsByTimestamp.forEach((timestamp, rows) -> {
              //logger.info("\t" + timestamp + " -->");
              //rows.forEach(row -> logger.info("\t\t[row]: tags: " + row.getString("tags") + ", valuetime: " + row.getTimestamp("valuetime").getTime() + ", value: " + row.getDouble("value")));
            //});
          //});
          //logger.info("[SCAN][/Rows grouped by tag AND timestamp]");
        //}
        //Map<String, Map<String, ResultContainer>> groupByResults = rowsByTagsandTimestamp.entrySet()
          //.stream().collect(Collectors.toMap(outerEntry -> outerEntry.getKey(), outerEntry -> {
            //String groupByTagsKey = outerEntry.getKey();
            //Map<String, String> resultTags = new HashMap<>(tagsMap);
            //((Map<String, String>)gson.fromJson(groupByTagsKey, jSonToMapType)).forEach((k,v) -> resultTags.merge(k,v,(v1,v2) -> v1));
            //String resultTagsJson = gson.toJson(resultTags);
            //return outerEntry.getValue().entrySet()
              //.stream().collect(Collectors.toMap(entry -> groupByTagsKey + ":" + entry.getKey(), entry -> {
                //switch (groupByFunction.toString()) {
                  //case "SUM":
                    //return new ResultContainer(metric, resultTagsJson, entry.getKey(), entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).sum());
                  //case "AVERAGE":
                    //return new ResultContainer(metric, resultTagsJson, entry.getKey(), entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).average().orElse(0));
                  //case "MAX":
                    //return new ResultContainer(metric, resultTagsJson, entry.getKey(), entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).max().orElse(0));
                  //case "MIN":
                    //return new ResultContainer(metric, resultTagsJson, entry.getKey(), entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).min().orElse(0));
                  //case "COUNT":
                    //return new ResultContainer(metric, resultTagsJson, entry.getKey(), entry.getValue().stream().count());
                  //default:
                    //throw new IllegalArgumentException("Unsupported groupByFunction: " + groupByFunction.toString());
                //}
              //},
              //(u, v) -> {
                //throw new IllegalStateException(String.format("Duplicate key %s", u));
              //},
              //LinkedHashMap::new));
          //},
          //(u, v) -> {
            //throw new IllegalStateException(String.format("Duplicate key %s", u));
          //},
          //LinkedHashMap::new));
          //.stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
            //switch (groupByFunction.toString()) {
              //case "SUM":
                //return entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).sum();
              //case "AVERAGE":
                //return entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).average().orElse(0);
              //case "MAX":
                //return entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).max().orElse(0);
              //case "MIN":
                //return entry.getValue().stream().mapToDouble(row -> row.getDouble("value")).min().orElse(0);
              //case "COUNT":
                //return entry.getValue().stream().count();
              //default:
                //throw new IllegalArgumentException("Unsupported groupByFunction: " + groupByFunction.toString());
            //}
          //}));
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
        logger.info("[INSERT] metric: " + metric + ", tags: " + new Gson().toJson(tagsAsStrings) + ", timestamp: " + timestamp + "(" + new Date(timestamp * 1000) + "), value: " + value);
      }
      if (missingreadsdebug) {
        logger.info("[INSERT] metric: " + metric + ", tags: " + new Gson().toJson(tagsAsStrings) + ", timestamp: " + timestamp + "(" + new Date(timestamp * 1000) + "), value: " + value);
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

  protected String rowToString(Row row) {
    return "[row] tags: " + row.getString("tags") + ", timestamp: " + row.getTimestamp("valuetime").getTime() + ", value: " + row.getDouble("value");
  }

  protected void logMap(Map<String, ?> mapToLog) {
    logMap(mapToLog, 0);
  }

  protected void logMap(Map<String, ?> mapToLog, Integer level) {
    mapToLog.forEach((key, value) -> {
      if (value instanceof Map) {
        logger.info(String.join("", Collections.nCopies(level, "\t")) + key + " -->");
        logMap((Map<String, ?>) value, level + 1);
      } else if (value instanceof List) {
        logger.info(String.join("", Collections.nCopies(level, "\t")) + key + " -->");
        ((List) value).forEach(item -> logger.info(String.join("", Collections.nCopies(level + 1, "\t")) + item));
      } else {
        logger.info(String.join("", Collections.nCopies(level, "\t")) + key + " --> value = " + value);
      }
    });
  }

  //protected void logMap(Map<String, ?> mapToLog) {
    //mapToLog.forEach((key, result) -> {
      //if (result instanceof List) {
        //logger.info("\t" + key + " -->");
        //((List)result).forEach(item -> logger.info("\t\t" + item));
      //} else {
        //logger.info("\t" + key + " --> result = " + result);
      //}
    //});
  //}

  //protected void logTwoLevelMap(Map<String, ? extends Map<String, ?>> twoLevelMap) {
    //twoLevelMap.forEach((firstLevelKey, secondLevelMap) -> {
      //logger.info(firstLevelKey + " -->");
      //logMap(secondLevelMap);
      ////secondLevelMap.forEach((secondLevelKey, result) -> {
        ////logger.info("\t" + secondLevelKey+ " --> result = " + result);
      ////});
    //});

  //}

  protected void logGroupByResults(Map<String, ? extends Map<String, ?>> groupByResults, Set<String> groupByTags, AggregationOperation groupByFunction) {
    logger.info("[SCAN][GroupBy Results][groupByTags = " + groupByTags + "][groupByFunction = " + groupByFunction.toString() + "]");
    logMap(groupByResults);
    //groupByResults.forEach((tagsGroup, resultByTimestamp) -> {
      //logger.info(tagsGroup + " -->");
      //resultByTimestamp.forEach((timestamp, result) -> {
        //logger.info("\t" + timestamp+ " --> result = " + result);
      //});
    //});
    logger.info("[SCAN][/GroupBy Result]");
  }
  
  /**
   * Aggregates Rows using the supplied aggregation function.
   *
   * "Rows" in this case can either mean actual Row class rows, or Map Entries,
   * or actually anything else, cos the function is generic. You have to 
   * pass in a function (usually in the form of a lamba) that takes as input
   * the element you are aggregating, and returns a Double, representing the value
   * for that "Row".
   * This is intended to be used as part of a single or multi-level groupingBy
   * operation (eg. to first group rows by timestamp, or tag, and then aggregate)
   *
   * @param aggregationFunction The aggregation function that should be used
   * @param mapToDouble The function that maps the "Row" to it's Double value
   * @return A Collector that will aggregate the rows and return a ? extends Number
   */
  protected <T> Collector<T, ?, ? extends Number> aggregateRows(AggregationOperation aggregationFunction, Function<T, Double> mapToDouble) {
    if (debug) { logger.info("[SCAN] Peforming Aggregation with " + aggregationFunction.toString() + " function"); }
    switch (aggregationFunction.toString()) {
      case "SUM":
        return Collectors.summingDouble(
            (T row) -> mapToDouble.apply(row));
      case "AVERAGE":
        return Collectors.averagingDouble(
            (T row) -> mapToDouble.apply(row));
      case "MAX":
        return Collectors.reducing(
            new Double(0),
            mapToDouble,
            Double::max);
      case "MIN":
        return Collectors.reducing(
            Double.POSITIVE_INFINITY,
            mapToDouble,
            Double::min);
      case "COUNT":
        return Collectors.counting();
      default:
        throw new IllegalArgumentException("Unsupported aggregationFunction: " + aggregationFunction.toString());
    }
  }

  protected Stream<Row> filterResultSetStreamByTags(Stream<Row> resultSetStream, Map<String, String> tagsMap) {
    // Basically if the configured tag count matches the number of tags provided in the
    // tagsMap, then we have a fully-qualified tagsMap for the query, meaning we
    // only get one time series, and there no need for filtering.
    // Otherwise, we need to filter client-side, because we would've been
    // given all the time series for the given metric.
    if (tagCount != tagsMap.size()) {
      if (debug) { logger.info("[SCAN][filtering by tags client-side] tags: " + tagsMap); }
      return resultSetStream.filter(row -> filterRowsByTags(row, tagsMap));
    } else { // no Filtering required, return input stream as-is
      return resultSetStream;
    }
  }

  /**
   * Filters Rows returned by a query based on a map of tags (key/value pairs).
   *
   * Intended to be used as a the predicate in a filter() call on the ResultSet stream
   * Note: Needs to be wrapped in a lambda to be able to pass the second parameter,
   *       eg. filter(row -> filterRowsByTags(row, tagsMap))
   *
   * @param row     The row to be filtered 
   * @param tagsMap The map of tags that a row should be matched against
   * @return true if row matches all entries in tagsMap, otherwise false
   */
  protected boolean filterRowsByTags(Row row, Map<String, String> tagsMap) {
    Map<String, String> rowTags = gson.fromJson(row.getString("tags"), Map.class);
    // Check if all the tags k/v pairs in the tags query
    // match those of the current row
    boolean rowMatch = tagsMap.entrySet().stream().allMatch(entry -> {
      return rowTags.get(entry.getKey()).equals(entry.getValue());
    });
    if (trace && rowMatch) { 
      logger.info("\t[filtering][filter] " + tagsMap);
      logger.info("\t[filtering][matching row] tags: " + row.getString("tags") + ", valuetime: " + row.getTimestamp("valuetime").getTime() + ", value: " + row.getDouble("value"));
    }
    return rowMatch;
  }

  /**
   * Groups Rows returned by a query based on a tag keys.
   *
   * Intended to be used as a the classifier in a Collectors.groupingBy() call on the ResultSet stream
   * Note: Needs to be wrapped in a lambda to be able to pass the second parameter,
   *       eg. Collectors.groupingBy(row -> groupRowByGroupByTags(row, groupByTags), ...)
   *
   * @param row         The row to be grouped
   * @param groupByTags The set of tag keys that a row should be grouped by 
   * @return The string with the tags (key+value) as JSON, that act as a token for grouping
   */
  protected String groupRowByGroupByTags(Row row, Set<String> groupByTags) {
    String tagsToGroupByJson = gson.toJson(groupedTagsForRow(row, groupByTags));
    if (trace) { logger.info("\t[grouping] tag(s) to group by: " + tagsToGroupByJson); }
    return tagsToGroupByJson;
  }

  protected Map<String, String> groupedTagsForRow(Row row, Set<String> groupByTags) {
    Map<String, String> rowTags = gson.fromJson(row.getString("tags"), jSonToMapType);
    return rowTags.entrySet().stream()
      .filter(entry -> groupByTags.contains(entry.getKey()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Groups Rows returned by a query based on timestamp.
   *
   * Intended to be used as a the classifier in a Collectors.groupingBy() call on the ResultSet stream
   *
   * @param row                       The row to be grouped
   * @return The string representation of the downsampled UNIX timestamp, that acts as a token for grouping
   */
  protected String groupRowByTimestamp(Row row) {
    return new Long(row.getTimestamp("valuetime").getTime()).toString();
  }

  /**
   * Groups Rows returned by a query based on a downsampled time window.
   *
   * Intended to be used as a the classifier in a Collectors.groupingBy() call on the ResultSet stream
   * Note: Needs to be wrapped in a lambda to be able to pass the second parameter,
   *       eg. Collectors.groupingBy(row -> {
   *                 groupRowByDownsampledTimestamp(row, downsamplingWindowLength, downsamplingWindowUnit)
   *               }, ...)
   *
   * @param row                       The row to be grouped
   * @param downsamplingWindowLength  The length of the downsampling window in downsamplingWindowUnit  
   * @param downsamplingWindowUnit    The TimeUnit of downsamplingWindowLength 
   * @return The string representation of the downsampled UNIX timestamp, that acts as a token for grouping
   */
  protected String groupRowByDownsampledTimestamp(Row row, int downsamplingWindowLength, TimeUnit downsamplingWindowUnit) {
    long timestamp = row.getTimestamp("valuetime").getTime();
    long groupingTimestamp = timestamp;
    switch (downsamplingWindowUnit) {
      case MILLISECONDS:
        groupingTimestamp = timestamp / downsamplingWindowLength;
        break;
      case SECONDS:
        groupingTimestamp = timestamp / (1000 * downsamplingWindowLength) * (1000 * downsamplingWindowLength);
        break;
      case MINUTES:
        groupingTimestamp = timestamp / (1000 * 60 * downsamplingWindowLength) * (1000 * 60 * downsamplingWindowLength);
        break;
      case HOURS:
        groupingTimestamp = timestamp / (1000 * 60 * 60 * downsamplingWindowLength) * (1000 * 60 * 60 * downsamplingWindowLength);
        break;
      case DAYS:
        groupingTimestamp = timestamp / (1000 * 60 * 60 * 24 * downsamplingWindowLength) * (1000 * 60 * 60 * 24 * downsamplingWindowLength);
        break;
      default:
        throw new IllegalArgumentException("Unsupported downsampling downsamplingWindowUnit: " + downsamplingWindowUnit);
    }
    if (trace) { 
      logger.info("\t[grouping row] timestamp: " + row.getTimestamp("valuetime").getTime() + ", value: " + row.getDouble("value") + " --> timeBucket = " + groupingTimestamp);
    }
    return new Long(groupingTimestamp).toString();
  }

  protected class ResultContainer {
    public String metric;
    public String tags;
    public long timestamp;
    public double value;

    //public static BinaryOperator<ResultContainer> sum = (a,b) -> {
      //a.value = a.value + b.value;
      //return a;
    //};

    protected ResultContainer(Row row) {
      this.metric = row.getString("metric");
      this.tags = row.getString("tags");
      this.timestamp = row.getTimestamp("valuetime").getTime();
      this.value = row.getDouble("value");
    }

    protected ResultContainer(String metric, String tags, String timestamp, double value) {
      this.metric = metric;
      this.tags = tags;
      this.timestamp = Long.parseLong(timestamp);
      this.value = value;
    }

    protected ResultContainer(String metric, String tags, long timestamp, double value) {
      this.metric = metric;
      this.tags = tags;
      this.timestamp = timestamp;
      this.value = value;
    }

    public String toString() {
      return "metric: " + metric + ", tags: " + tags + ", timestamp: " + timestamp + ", value: " + value;
    }

    public String toStringWithoutValue() {
      return "metric: " + metric + ", tags: " + tags + ", timestamp: " + timestamp;
    }
  }
}
