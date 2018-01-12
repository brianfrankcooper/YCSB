/*
 * Copyright (c) 2015 - 2017 Andreas Bader All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * InfluxDB client for YCSB framework.
 * Tagfilter seems a bit problematic/not working correctly
 */
public class InfluxDBClient extends TimeseriesDB {

  // influx-binding specific properties
  private static final String PROPERTY_IP = "ip";
  private static final String PROPERTY_PORT = "port";

  private static final String PROPERTY_DB_NAME = "dbName";
  private static final String PROPERTY_DB_NAME_DEFAULT = "ycsb";

  private static final String PROPERTY_BATCH = "batch";
  private static final String PROPERTY_BATCH_DEFAULT = "false";

  private static final String PROPERTY_RETENTION_POLICY = "retentionPolicy";
  private static final String PROPERTY_RETENTION_POLICY_DEFAULT = "autogen";

  private static final String RETENTION_POLICY_DURATION = "INF";
  private static final String RETENTION_POLICY_SHARD_DURATION = "2d";
  private static final int RETENTION_POLICY_REPLICATION_FACTOR = 1;
  private static final boolean RETENTION_POLICY_IS_DEFAULT = false;

  // influxdb connection relevant properties and binding configuration properties
  private String ip;
  private int port;
  private String dbName;
  private String retentionPolicy;
  private boolean groupBy = true;
  private final String valueFieldName = "value";

  private InfluxDB client;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize protected fields we need for workload
    super.init();
    try {
      if (!getProperties().containsKey(PROPERTY_PORT)) {
        throw new DBException("No port given, abort.");
      }
      if (!getProperties().containsKey(PROPERTY_IP)) {
        throw new DBException("No ip given, abort.");
      }

      dbName = getProperties().getProperty(PROPERTY_DB_NAME, PROPERTY_DB_NAME_DEFAULT);
      retentionPolicy = getProperties().getProperty(PROPERTY_RETENTION_POLICY, PROPERTY_RETENTION_POLICY_DEFAULT);

      // Must be set, if it blows up, we can error out
      port = Integer.parseInt(getProperties().getProperty(PROPERTY_PORT));
      ip = getProperties().getProperty(PROPERTY_IP);

      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
      if (!test) {
        // TODO allow passing credentials?
        this.client = InfluxDBFactory.connect(String.format("http://%s:%s", ip, port));
        if (debug) {
          this.client = this.client.setLogLevel(InfluxDB.LogLevel.FULL);
        }
        boolean batch = Boolean.parseBoolean(getProperties().getProperty(PROPERTY_BATCH, PROPERTY_BATCH_DEFAULT));
        if (batch) {
          this.client = this.client.enableBatch(10, 1000, TimeUnit.MILLISECONDS);
        }
        this.client.ping();

        // ensure database and retention policy exist:
        if (!client.databaseExists(dbName)) {
          client.createDatabase(dbName);
        }
        Query retentionPolicies = new Query(String.format("SHOW RETENTION POLICIES ON \"%s\"", dbName), dbName);
        QueryResult result = client.query(retentionPolicies);
        if (result.hasError()) {
          throw new DBException("Could not verify retention policy exists");
        }
        boolean exists = false;
        for (QueryResult.Result res : result.getResults()) {
          if (res.hasError()) {
            throw new DBException("Could not verify retention policy exists");
          }
          for (QueryResult.Series series : res.getSeries()) {
            for (List<Object> values : series.getValues()) {
              if (values.get(0).equals(retentionPolicy)) {
                exists = true;
              }
            }
          }
        }
        if (!exists) {
          client.createRetentionPolicy(retentionPolicy, dbName, RETENTION_POLICY_DURATION,
              RETENTION_POLICY_SHARD_DURATION, RETENTION_POLICY_REPLICATION_FACTOR, RETENTION_POLICY_IS_DEFAULT);
        }
      }
    } catch (retrofit.RetrofitError e) {
      throw new DBException(String.format("Can't connect to %s:%s.)", ip, port) + e);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      this.client.close();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }


  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.isEmpty()) {
      return Status.BAD_REQUEST;
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
    }
    String tagFilter = buildTagFilter(tags);

    String fqMetric = String.format("%s.%s.%s", dbName, retentionPolicy, metric);
    // InfluxDB can not use milliseconds or nanoseconds, it uses microseconds or seconds (or greater).
    // See https://docs.influxdata.com/influxdb/v0.8/api/query_language/.
    // u stands for microseconds. Since getNanos() seems unfair because no other TSDB uses it, we just add three zeros
    Query query = new Query(String.format("SELECT * FROM %s WHERE time = %s000u%s", fqMetric,
        timestamp, tagFilter), dbName);
    if (debug) {
      System.out.println("Query: " + query.getCommand());
    }
    if (test) {
      return Status.OK;
    }
    QueryResult qr = this.client.query(query);
    if (qr.hasError()) {
      System.err.println("ERROR: Error occured while Querying: " + qr.getError());
      return Status.ERROR;
    }
    if (qr.getResults().isEmpty()) {
      // allowed to happen!
      return Status.NOT_FOUND;
    }
    for (QueryResult.Result result : qr.getResults()) {
      if (result.hasError()) {
        System.err.println("ERROR: Error occured while iterating query results: " + result.getError());
        return Status.ERROR;
      }
      // I'm not even going to ask ...
      if (result.getSeries() == null) {
        continue;
      }
      for (QueryResult.Series series : result.getSeries()) {
        if (series.getValues().size() != 1) {
          if (series.getValues().isEmpty()) {
            System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
            return Status.NOT_FOUND;
          } else {
            // FIXME data integrity check should blow up here?
            System.err.printf("ERROR: Found more than one value for metric: %s for timestamp: %s.%n",
                metric, timestamp);
          }
        }
        // FIXME integrity checks that we actually got the result we wanted?
      }
    }
    return Status.OK;
  }


  @Override
  protected Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                      TimeseriesDB.AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric.isEmpty()) {
      return Status.BAD_REQUEST;
    }
    if (startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }

    String tagFilter = buildTagFilter(tags);
    String fieldStr = "*";
    if (aggreg != TimeseriesDB.AggregationOperation.NONE) {
      if (aggreg == TimeseriesDB.AggregationOperation.AVERAGE) {
        fieldStr = "MEAN(" + this.valueFieldName + ")";
      } else if (aggreg == TimeseriesDB.AggregationOperation.COUNT) {
        fieldStr = "COUNT(" + this.valueFieldName + ")";
      } else if (aggreg == TimeseriesDB.AggregationOperation.SUM) {
        fieldStr = "SUM(" + this.valueFieldName + ")";
      }
    }
    String groupByStr = "";
    if (this.groupBy && timeValue != 0 && aggreg != TimeseriesDB.AggregationOperation.NONE) {
      groupByStr = " GROUP BY time(" + timeValue + "%s)";
      if (timeUnit == TimeUnit.MILLISECONDS) {
        groupByStr = " GROUP BY time(" + TimeUnit.MICROSECONDS.convert(timeValue, timeUnit) + "u)";
        System.err.println("WARNING: InfluxDB will probably not work correctly on low millisecond timeunits!");
      } else if (timeUnit == TimeUnit.SECONDS) {
        groupByStr = String.format(groupByStr, "s");
      } else if (timeUnit == TimeUnit.MINUTES) {
        groupByStr = String.format(groupByStr, "m");
      } else if (timeUnit == TimeUnit.HOURS) {
        groupByStr = String.format(groupByStr, "h");
      } else if (timeUnit == TimeUnit.DAYS) {
        groupByStr = String.format(groupByStr, "d");
      } else {
        groupByStr = "GROUP BY time(" + TimeUnit.MICROSECONDS.convert(timeValue, timeUnit) + "u)";
        System.err.println("WARNING: Unknown timeunit " + timeUnit.toString() + ", converting to milliseconds." +
            "InfluxDB may not work correctly on low millisecond values.");
      }
    }
    // InfluxDB can not use milliseconds or nanoseconds, it uses microseconds or seconds (or greater).
    // See https://docs.influxdata.com/influxdb/v0.8/api/query_language/.
    // u stands for microseconds. Since getNanos() seems unfair because no other TSDB uses it, we just add three zeros
    Query query = new Query(String.format("SELECT %s FROM %s WHERE time >= %s000u AND time <= %s000u%s%s", fieldStr,
        metric, startTs, endTs, tagFilter.toString(), groupByStr), dbName);
    if (debug) {
      System.out.println("Query: " + query.getCommand());
    }
    if (test) {
      return Status.OK;
    }
    QueryResult qr = this.client.query(query);
    if (qr.hasError()) {
      System.err.println("ERROR: Error occured while Querying: " + qr.getError() + " Query: " + query.getCommand());
      return Status.ERROR;
    }
    if (qr.getResults().isEmpty()) {
      // allowed to happen!
      return Status.NOT_FOUND;
    }
    Boolean found = false;
    for (QueryResult.Result result : qr.getResults()) {
      if (result.getSeries() == null) {
        return Status.ERROR;
      }
      for (QueryResult.Series series : result.getSeries()) {
        if (series.getValues().isEmpty()) {
          return Status.NOT_FOUND;
        }
        for (List<Object> values : series.getValues()) {
          // null is okay, as it means 0 for sum,count,avg
          found = true;
          if (values.isEmpty()) {
            return Status.NOT_FOUND;
          }
        }
      }

    }
    if (!found) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }

  @Override
  protected Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.isEmpty()) {
      return Status.BAD_REQUEST;
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      // TODO: use timestampUnit if possible
      Point.Builder pb = Point.measurement(metric).time(timestamp, TimeUnit.MILLISECONDS);
      for (Map.Entry entry : tags.entrySet()) {
        pb = pb.addField(entry.getKey().toString(), entry.getValue().toString());
      }
      pb = pb.addField(this.valueFieldName, String.valueOf(value));
      if (test) {
        return Status.OK;
      }
      this.client.write(this.dbName, retentionPolicy, pb.build());
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      if (debug) {
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED; // makes no sense in a TSDB
  }

  @Override
  public Status delete(String table, String key) {
    String[] parts = key.split(deleteDelimiter);
    // influxdb structure doesn't match YCSB expectations
    String ignored = parts[0];

    Map<String, List<String>> tagQuerySpecifier = Arrays.stream(parts).skip(1)
        .map(s -> s.split(tagPairDelimiter))
        .collect(Collectors.groupingBy(s -> s[0], Collectors.mapping(s -> s[1], Collectors.toList())));

    String tagFilter = buildTagFilter(tagQuerySpecifier);

    Query delete = new Query(String.format("DELETE FROM '%s' WHERE %s", table, tagFilter), this.dbName);
    QueryResult result = client.query(delete);

    if (result.hasError()) {
      System.err.println("ERROR: Error in processing delete to metric: " + table + result.getError());
      return Status.ERROR;
    }
    // We don't even get to know how many records we deleted
    return Status.OK;
  }
}

