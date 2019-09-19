/*
 * Copyright (c) 2018 YCSB Contributors All rights reserved.
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
package site.ycsb;

import site.ycsb.generator.Generator;
import site.ycsb.generator.IncrementingPrintableStringGenerator;
import site.ycsb.workloads.TimeSeriesWorkload;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class to adapt the default ycsb DB interface to Timeseries databases.
 * This class is mostly here to be extended by Timeseries dataabases
 * originally developed by Andreas Bader in <a href="https://github.com/TSDBBench/YCSB-TS">YCSB-TS</a>.
 * <p>
 * This class is mostly parsing the workload information passed through the default ycsb interface
 * according to the information outlined in {@link TimeSeriesWorkload}.
 * It also contains some minor utility methods relevant to Timeseries databases.
 * </p>
 *
 * @implSpec It's vital to call <tt>super.init()</tt> when overwriting the init method
 * to correctly initialize the workload-parsing.
 */
public abstract class TimeseriesDB extends DB {

  // defaults for downsampling. Basically we ignore it
  private static final String DOWNSAMPLING_FUNCTION_PROPERTY_DEFAULT = "NONE";
  private static final String DOWNSAMPLING_INTERVAL_PROPERTY_DEFAULT = "0";

  // debug property loading
  private static final String DEBUG_PROPERTY = "debug";
  private static final String DEBUG_PROPERTY_DEFAULT = "false";

  // test property loading
  private static final String TEST_PROPERTY = "test";
  private static final String TEST_PROPERTY_DEFAULT = "false";

  // Workload parameters that we need to parse this
  protected String timestampKey;
  protected String valueKey;
  protected String tagPairDelimiter;
  protected String queryTimeSpanDelimiter;
  protected String deleteDelimiter;
  protected TimeUnit timestampUnit;
  protected String groupByKey;
  protected String downsamplingKey;
  protected Integer downsamplingInterval;
  protected AggregationOperation downsamplingFunction;

  // YCSB-parameters
  protected boolean debug;
  protected boolean test;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
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
    downsamplingFunction = TimeseriesDB.AggregationOperation.valueOf(getProperties()
        .getProperty(TimeSeriesWorkload.DOWNSAMPLING_FUNCTION_PROPERTY, DOWNSAMPLING_FUNCTION_PROPERTY_DEFAULT));
    downsamplingInterval = Integer.valueOf(getProperties()
        .getProperty(TimeSeriesWorkload.DOWNSAMPLING_INTERVAL_PROPERTY, DOWNSAMPLING_INTERVAL_PROPERTY_DEFAULT));

    test = Boolean.parseBoolean(getProperties().getProperty(TEST_PROPERTY, TEST_PROPERTY_DEFAULT));
    debug = Boolean.parseBoolean(getProperties().getProperty(DEBUG_PROPERTY, DEBUG_PROPERTY_DEFAULT));
  }

  @Override
  public final Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, List<String>> tagQueries = new HashMap<>();
    Long timestamp = null;
    for (String field : fields) {
      if (field.startsWith(timestampKey)) {
        String[] timestampParts = field.split(tagPairDelimiter);
        if (timestampParts[1].contains(queryTimeSpanDelimiter)) {
          // Since we're looking for a single datapoint, a range of timestamps makes no sense.
          // As we cannot throw an exception to bail out here, we return `BAD_REQUEST` instead.
          return Status.BAD_REQUEST;
        }
        timestamp = Long.valueOf(timestampParts[1]);
      } else {
        String[] queryParts = field.split(tagPairDelimiter);
        tagQueries.computeIfAbsent(queryParts[0], k -> new ArrayList<>()).add(queryParts[1]);
      }
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
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
  protected abstract Status read(String metric, long timestamp, Map<String, List<String>> tags);

  /**
   * @inheritDoc
   * @implNote this method parses the information passed to it and subsequently passes it to the modified
   * interface at {@link #scan(String, long, long, Map, AggregationOperation, int, TimeUnit)}
   */
  @Override
  public final Status scan(String table, String startkey, int recordcount, Set<String> fields,
                           Vector<HashMap<String, ByteIterator>> result) {
    Map<String, List<String>> tagQueries = new HashMap<>();
    TimeseriesDB.AggregationOperation aggregationOperation = TimeseriesDB.AggregationOperation.NONE;
    Set<String> groupByFields = new HashSet<>();

    boolean rangeSet = false;
    long start = 0;
    long end = 0;
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
        rangeSet = true;
        start = Long.valueOf(rangeParts[0]);
        end = Long.valueOf(rangeParts[1]);
      } else if (field.startsWith(groupByKey)) {
        String groupBySpecifier = field.split(tagPairDelimiter)[1];
        aggregationOperation = TimeseriesDB.AggregationOperation.valueOf(groupBySpecifier);
      } else if (field.startsWith(downsamplingKey)) {
        String downsamplingSpec = field.split(tagPairDelimiter)[1];
        // apparently that needs to always hold true:
        if (!downsamplingSpec.equals(downsamplingFunction.toString() + downsamplingInterval.toString())) {
          System.err.print("Downsampling specification for Scan did not match configured downsampling");
          return Status.BAD_REQUEST;
        }
      } else {
        String[] queryParts = field.split(tagPairDelimiter);
        if (queryParts.length == 1) {
          // we should probably warn about this being ignored...
          System.err.println("Grouping by arbitrary series is currently not supported");
          groupByFields.add(field);
        } else {
          tagQueries.computeIfAbsent(queryParts[0], k -> new ArrayList<>()).add(queryParts[1]);
        }
      }
    }
    if (!rangeSet) {
      return Status.BAD_REQUEST;
    }
    return scan(table, start, end, tagQueries, downsamplingFunction, downsamplingInterval, timestampUnit);
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
  protected abstract Status scan(String metric, long startTs, long endTs, Map<String, List<String>> tags,
                                 AggregationOperation aggreg, int timeValue, TimeUnit timeUnit);

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
    // not supportable for general TSDBs
    // can be explicitly overwritten in inheriting classes
  }

  @Override
  public final Status insert(String table, String key, Map<String, ByteIterator> values) {
    NumericByteIterator tsContainer = (NumericByteIterator) values.remove(timestampKey);
    NumericByteIterator valueContainer = (NumericByteIterator) values.remove(valueKey);
    if (valueContainer.isFloatingPoint()) {
      return insert(table, tsContainer.getLong(), valueContainer.getDouble(), values);
    } else {
      return insert(table, tsContainer.getLong(), valueContainer.getLong(), values);
    }
  }

  /**
   * Insert a record into the database. Any tags/tagvalue pairs in the specified tagmap and the given value will be
   * written into the record with the specified timestamp.
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to insert.
   * @param value     The actual value to insert.
   * @param tags      A Map of tag/tagvalue pairs to insert as tags
   * @return A {@link Status} detailing the outcome of the insert
   */
  protected abstract Status insert(String metric, long timestamp, long value, Map<String, ByteIterator> tags);

  /**
   * Insert a record in the database. Any tags/tagvalue pairs in the specified tagmap and the given value will be
   * written into the record with the specified timestamp.
   *
   * @param metric    The name of the metric
   * @param timestamp The timestamp of the record to insert.
   * @param value     actual value to insert
   * @param tags      A HashMap of tag/tagvalue pairs to insert as tags
   * @return A {@link Status} detailing the outcome of the insert
   */
  protected abstract Status insert(String metric, long timestamp, double value, Map<String, ByteIterator> tags);

  /**
   * NOTE: This operation is usually <b>not</b> supported for Time-Series databases.
   * Deletion of data is often instead regulated through automatic cleanup and "retention policies" or similar.
   *
   * @return Status.NOT_IMPLEMENTED or a {@link Status} specifying the outcome of deletion
   * in case the operation is supported.
   */
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Examines the given {@link Properties} and returns an array containing the Tag Keys
   * (basically matching column names for traditional Relational DBs) that are detailed in the workload specification.
   * See {@link TimeSeriesWorkload} for how these are generated.
   * <p>
   * This method is intended to be called during the initialization phase to create a table schema
   * for DBMS that require such a schema before values can be inserted (or queried)
   *
   * @param properties The properties detailing the workload configuration.
   * @return An array of strings specifying all allowed TagKeys (or column names)
   * except for the "value" and the "timestamp" column name.
   * @implSpec WARNING this method must exactly match how tagKeys are generated by the {@link TimeSeriesWorkload},
   * otherwise databases requiring this information will most likely break!
   */
  protected static String[] getPossibleTagKeys(Properties properties) {
    final int tagCount = Integer.parseInt(properties.getProperty(TimeSeriesWorkload.TAG_COUNT_PROPERTY,
        TimeSeriesWorkload.TAG_COUNT_PROPERTY_DEFAULT));
    final int tagKeylength = Integer.parseInt(properties.getProperty(TimeSeriesWorkload.TAG_KEY_LENGTH_PROPERTY,
        TimeSeriesWorkload.TAG_KEY_LENGTH_PROPERTY_DEFAULT));

    Generator<String> tagKeyGenerator = new IncrementingPrintableStringGenerator(tagKeylength);
    String[] tagNames = new String[tagCount];
    for (int i = 0; i < tagCount; i++) {
      tagNames[i] = tagKeyGenerator.nextValue();
    }
    return tagNames;
  }


  /**
   * An enum containing the possible aggregation operations.
   * Not all of these operations are required to be supported by implementing classes.
   * <p>
   * Aggregations are applied when using the <tt>SCAN</tt> operation on a range of timestamps.
   * That way the result set is reduced from multiple records into
   * a single one or one record for each group specified through <tt>GROUP BY</tt> clauses.
   */
  public enum AggregationOperation {
    /**
     * No aggregation whatsoever. Return the results as a full table
     */
    NONE,
    /**
     * Sum the values of the matching records when calculating the value.
     * GroupBy criteria apply where relevant for sub-summing.
     */
    SUM,
    /**
     * Calculate the arithmetic mean over the value across matching records when calculating the value.
     * GroupBy criteria apply where relevant for group-targeted averages
     */
    AVERAGE,
    /**
     * Count the number of matching records and return that as value.
     * GroupBy criteria apply where relevant.
     */
    COUNT,
    /**
     * Return only the maximum of the matching record values.
     * GroupBy criteria apply and result in group-based maxima.
     */
    MAX,
    /**
     * Return only the minimum of the matching record values.
     * GroupBy criteria apply and result in group-based minima.
     */
    MIN;
  }
}
