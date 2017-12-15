/**
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
import com.yahoo.ycsb.workloads.TimeSeriesWorkload;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB client for YCSB framework.
 * Tagfilter seems a bit problematic/not working correctly
 */
public class InfluxDBClient extends DB {

  private static final String RETENTION_POLICY = "autogen";

  private String ip = "localhost";
  private String dbName = "testdb";
  private int port = 8086;
  private boolean debug = false;
  private InfluxDB client;
  private boolean batch = false;
  private boolean groupBy = true;
  private boolean test = false;
  private String valueFieldName = "value"; // in which field should the value be?

  private String timestampKey;
  private String valueKey;
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  private String deleteDelimiter;
  private TimeUnit timestampUnit;
  private String groupByKey;
  private String downsamplingKey;
  private Integer downsamplingInterval;
  private AggregationOperation downsamplingFunction;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    try {
      // taken from BasicTSDB
      timestampKey = getProperties().getProperty(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY,
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT);
      valueKey = getProperties().getProperty(
          TimeSeriesWorkload.VALUE_KEY_PROPERTY,
          TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT);
      tagPairDelimiter = getProperties().getProperty(
          TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY,
          TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
      queryTimeSpanDelimiter = getProperties().getProperty(
          TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
          TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
      deleteDelimiter = getProperties().getProperty(
          TimeSeriesWorkload.DELETE_DELIMITER_PROPERTY,
          TimeSeriesWorkload.DELETE_DELIMITER_PROPERTY_DEFAULT);
      timestampUnit = TimeUnit.valueOf(getProperties().getProperty(
          TimeSeriesWorkload.TIMESTAMP_UNITS_PROPERTY,
          TimeSeriesWorkload.TIMESTAMP_UNITS_PROPERTY_DEFAULT));
      groupByKey = getProperties().getProperty(
          TimeSeriesWorkload.GROUPBY_KEY_PROPERTY,
          TimeSeriesWorkload.GROUPBY_KEY_PROPERTY_DEFAULT);
      downsamplingKey = getProperties().getProperty(
          TimeSeriesWorkload.DOWNSAMPLING_KEY_PROPERTY,
          TimeSeriesWorkload.DOWNSAMPLING_KEY_PROPERTY_DEFAULT);
      downsamplingFunction = AggregationOperation.valueOf(getProperties()
          .getProperty(TimeSeriesWorkload.DOWNSAMPLING_FUNCTION_PROPERTY, "NONE"));
      downsamplingInterval = Integer.valueOf(getProperties()
          .getProperty(TimeSeriesWorkload.DOWNSAMPLING_INTERVAL_PROPERTY, "0"));


      test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
      batch = Boolean.parseBoolean(getProperties().getProperty("batch", "false"));
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
      dbName = getProperties().getProperty("dbName", dbName);
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);
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
        if (this.batch) {
          this.client = this.client.enableBatch(10, 1000, TimeUnit.MILLISECONDS);
        }
        this.client.ping();
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
    this.client.close();
  }

  /*
  For calls to DB.read(String, String, Set, Map) and DB.scan(String, String, int, Set, Vector),
   timestamps are encoded in a StringByteIterator in a key/value format with the tagpairdelimiter separator.
   E.g YCSBTS=1483228800.

   If querytimespan has been set to a positive value then the value will include a range with
   the starting (oldest) timestamp followed by the querytimespandelimiter separator and the ending timestamp.
   E.g. YCSBTS=1483228800-1483228920.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, List<String>> tagQueries = new HashMap<>();
    Long timestamp = null;
    for (String field : fields) {
      if (field.startsWith(timestampKey)) {
        String[] timestampParts = field.split(tagPairDelimiter);
        if (timestampParts[1].contains(queryTimeSpanDelimiter)) {
          // seems like this should be a more elaborate query.
          // for now we don't support querying ranges
          // TODO: Support Timestamp range queries
          return Status.NOT_IMPLEMENTED;
        }
        timestamp = Long.valueOf(timestampParts[1]);
      } else {
        String[] queryParts = field.split(tagPairDelimiter);
        // TODO tech-debt after java 7 support is dropped
        if (tagQueries.containsKey(queryParts[0])) {
          tagQueries.get(queryParts[0]).add(queryParts[1]);
        } else {
          List<String> tags = new ArrayList<>();
          tags.add(queryParts[1]);
          tagQueries.put(queryParts[0], tags);
        }
      }
    }

    return read(table, timestamp, tagQueries);
  }

  /**
   * Read a record from the database. Each value from the result will be stored in a HashMap
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to read.
   * @param tags      actual tags that were want to receive (can be empty)
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric == "") {
      return Status.BAD_REQUEST;
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
    }
    StringBuilder tagFilter = new StringBuilder();
    // TODO technical debt after dropping java 7
    for (String tag : tags.keySet()) {
      tagFilter.append(" AND ( ");
      for (String tagname : tags.get(tag)) {
        tagFilter.append(String.format(" OR %s = '%s'", tag, tagname));
      }
      tagFilter = new StringBuilder(tagFilter.toString().replaceFirst(" OR ", ""));
      tagFilter.append(" )");
    }

    String fqMetric = String.format("%s.%s.%s", dbName, RETENTION_POLICY, metric);
    // InfluxDB can not use milliseconds or nanoseconds, it uses microseconds or seconds (or greater).
    // See https://docs.influxdata.com/influxdb/v0.8/api/query_language/.
    // u stands for microseconds. Since getNanos() seems unfair because no other TSDB uses it, we just add three zeros
    Query query = new Query(String.format("SELECT * FROM %s WHERE time = %s000u%s", fqMetric,
        timestamp, tagFilter.toString()), dbName);
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
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    Map<String, List<String>> tagQueries = new HashMap<>();
    Long start = null;
    Long end = null;
    AggregationOperation aggregationOperation = AggregationOperation.NONE;
    Set<String> groupByFields = new HashSet<>();

    for (String field : fields) {
      if (field.startsWith(timestampKey)) {
        String[] timestampParts = field.split(tagPairDelimiter);
        if (!timestampParts[1].contains(queryTimeSpanDelimiter)) {
          // seems like this should be a more elaborate query.
          // for now we don't support scanning single timestamps
          // TODO: Support Timestamp range queries
          return Status.NOT_IMPLEMENTED;
        }
        String[] rangeParts = timestampParts[1].split(queryTimeSpanDelimiter);
        start = Long.valueOf(rangeParts[0]);
        end = Long.valueOf(rangeParts[1]);
      } else if (field.startsWith(groupByKey)) {
        String groupBySpecifier = field.split(tagPairDelimiter)[1];
        aggregationOperation = AggregationOperation.valueOf(groupBySpecifier);
      } else if (field.startsWith(downsamplingKey)) {
        String downsamplingSpec = field.split(tagPairDelimiter)[1];
        // apparently that needs to always hold true:
        if (!downsamplingSpec.equals(downsamplingFunction.toString() + downsamplingInterval.toString())) {
          // FIXME instead return BAD_REQUEST?
          System.err.print("Downsampling specification for Scan did not match configured downsampling");
        }
      } else {
        String[] queryParts = field.split(tagPairDelimiter);
        if (queryParts.length == 1) {
          // we should probably warn about this being ignored...
          System.err.println("Grouping by arbitrary series is currently not supported");
          groupByFields.add(field);
        } else if (tagQueries.containsKey(queryParts[0])) {
        // TODO tech-debt after java 7 support is dropped
          tagQueries.get(queryParts[0]).add(queryParts[1]);
        } else {
          List<String> tags = new ArrayList<>();
          tags.add(queryParts[1]);
          tagQueries.put(queryParts[0], tags);
        }
      }
    }
    return scan(table, start, end, tagQueries, downsamplingFunction, downsamplingInterval, timestampUnit);
  }

  private enum AggregationOperation {
    NONE, SUM, AVERAGE, COUNT
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
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric == "") {
      return Status.BAD_REQUEST;
    }
    if (startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    StringBuilder tagFilter = new StringBuilder();
    for (String tag : tags.keySet()) {
      tagFilter.append(" AND ( ");
      StringBuilder tempTagFilter = new StringBuilder(); // for OR Clauses
      for (String tagName : tags.get(tag)) {
        tempTagFilter.append(String.format(" OR %s = '%s'", tag, tagName));
      }
      tagFilter.append(tempTagFilter.toString().replaceFirst(" OR ", ""));
      tagFilter.append(" )");
    }
    String fieldStr = "*";
    if (aggreg != AggregationOperation.NONE) {
      if (aggreg == AggregationOperation.AVERAGE) {
        fieldStr = "MEAN(" + this.valueFieldName + ")";
      } else if (aggreg == AggregationOperation.COUNT) {
        fieldStr = "COUNT(" + this.valueFieldName + ")";
      } else if (aggreg == AggregationOperation.SUM) {
        fieldStr = "SUM(" + this.valueFieldName + ")";
      }
    }
    String groupByStr = "";
    if (this.groupBy && timeValue != 0 && aggreg != AggregationOperation.NONE) {
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
        if (series.getValues().size() == 0) {
          return Status.NOT_FOUND;
        }
        for (List<Object> obj : series.getValues()) {
          // null is okay, as it means 0 for sum,count,avg
//                    if (obj.get(obj.size()-1) != null) {
          found = true;
//                    }
          if (obj.size() == 0) {
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
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    NumericByteIterator tsContainer = (NumericByteIterator) values.remove(timestampKey);
    NumericByteIterator valueContainer = (NumericByteIterator) values.remove(valueKey);
    if (!valueContainer.isFloatingPoint()) {
      // non-double values are not supported by the adapter
      return Status.BAD_REQUEST;
    }
    return insert(table, tsContainer.getLong(), valueContainer.getDouble(), values);
  }

  /**
   * Insert a record in the database. Any tags/tagvalue pairs in the specified tags HashMap and the given value
   * will be written into the record with the specified timestamp
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to insert.
   * @param value     actual value to insert
   * @param tags      A HashMap of tag/tagvalue pairs to insert as tagsmv c
   * @return A {@link Status} detailing the outcome of the insert
   */
  private Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric == "") {
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
      this.client.write(this.dbName, RETENTION_POLICY, pb.build());
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Properties getProperties() {
    return super.getProperties();
  }

  @Override
  public void setProperties(Properties p) {
    super.setProperties(p);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED; // makes no sense in a TSDB
  }

  @Override
  public Status delete(String table, String key) {
    // FIXME needs to be implemented
    return Status.NOT_IMPLEMENTED;
  }
}

