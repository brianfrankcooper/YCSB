package com.yahoo.ycsb;

import com.yahoo.ycsb.workloads.TimeSeriesWorkload;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
          // seems like this should be a more elaborate query.
          // for now we don't support querying ranges
          // TODO: Support Timestamp range queries
          return Status.NOT_IMPLEMENTED;
        }
        timestamp = Long.valueOf(timestampParts[1]);
      } else {
        String[] queryParts = field.split(tagPairDelimiter);
        tagQueries.computeIfAbsent(queryParts[0], k -> new ArrayList<>()).add(queryParts[1]);
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
  protected abstract Status read(String metric, Long timestamp, Map<String, List<String>> tags);

  /**
   * @inheritDoc
   *
   * @implNote this method parses the information passed to it and subsequently passes it to the modified
   * interface at {@link #scan(String, Long, Long, Map, AggregationOperation, int, TimeUnit)}
   */
  @Override
  public final Status scan(String table, String startkey, int recordcount, Set<String> fields,
                           Vector<HashMap<String, ByteIterator>> result) {
    Map<String, List<String>> tagQueries = new HashMap<>();
    Long start = null;
    Long end = null;
    TimeseriesDB.AggregationOperation aggregationOperation = TimeseriesDB.AggregationOperation.NONE;
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
        aggregationOperation = TimeseriesDB.AggregationOperation.valueOf(groupBySpecifier);
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
        } else {
          tagQueries.computeIfAbsent(queryParts[0], k -> new ArrayList<>()).add(queryParts[1]);
        }
      }
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
  protected abstract Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
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
  protected abstract Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags);

  // must be explicitly overridden, since an intermediary parser is probably too much work ...
  public abstract Status delete(String table, String key);

  protected final String buildTagFilter(Map<String, List<String>> tags) {
    StringBuilder tagFilter = new StringBuilder();
    for (String tag : tags.keySet()) {
      tagFilter.append(" AND ( ");
      tagFilter.append(tags.get(tag).stream()
          .map(t -> String.format("%s = '%s'", tag, t))
          .collect(Collectors.joining(" OR ")));
      tagFilter.append(" )");
    }
    return tagFilter.toString();
  }

  /**
   * An enum containing the possible aggregation operations.
   */
  public enum AggregationOperation {
    NONE, SUM, AVERAGE, COUNT
  }
}
