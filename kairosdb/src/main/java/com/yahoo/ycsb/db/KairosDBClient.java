/*
 * Copyright (c) 2015 - 2018 Andreas Bader, 2018 YCSB Contributors All rights reserved.
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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.*;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * KairosDB client for YCSB framework.
 */
public class KairosDBClient extends TimeseriesDB {

  private static final String PORT_PROPERTY = "port";
  private static final String PORT_PROPERTY_DEFAULT = "8080";

  private static final String IP_PROPERTY = "ip";
  private static final String IP_PROPERTY_DEFAULT = "localhost";

  private HttpClient client = null;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    final Properties properties = getProperties();
    if (!properties.containsKey(PORT_PROPERTY) && !test) {
      throwMissingProperty(PORT_PROPERTY);
    }
    if (!properties.containsKey(IP_PROPERTY) && !test) {
      throwMissingProperty(IP_PROPERTY);
    }

    int port;
    try {
      port = Integer.parseInt(properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
    } catch (NumberFormatException e) {
      throw new DBException(e);
    }
    String ip = properties.getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);

    if (debug) {
      LOGGER.info("The following properties are given: ");
      for (String element : properties.stringPropertyNames()) {
        LOGGER.info("{}: {}", element, properties.getProperty(element));
      }
    }

    try {
      if (!test) {
        client = new HttpClient(String.format("http://%s:%s", ip, port));
      }
    } catch (MalformedURLException e) {
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
      if (!test) {
        client.shutdown();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    // Problem: You cant ask for a timestamp at TS=x, you need to give a range.
    // So: Begin: timestamp, End: timestamp + 1 ms
    // We may get more than that, but we just take the right one
    // There could also be more of them, so count
    QueryBuilder builder = QueryBuilder.getInstance();
    QueryMetric qm = builder.setStart(new Timestamp(timestamp))
        .setEnd(new Timestamp(timestamp + 1))
        .addMetric(metric);
    tags.forEach((key, value) -> qm.addTag(key, value.toArray(new String[0])));
    try {
      // No idea what that '******************* Type=number' Error means, seems to be from GSON or Apache httpclient
      // Old Debug Message in Kairos DB Client, see https://github.com/kairosdb/kairosdb-client/issues/43
      // Fixed in https://github.com/kairosdb/kairosdb-client/commit/e1b2ea9a0fef0ed67812580502d5faf4aa125e17
      // but no Maven version?
      // 31.10.15 -> seems to be fixed in mvn version?
      // 23.02.18 -> there's been two minor releases. Check fixes??
      if (test) {
        return Status.OK;
      }
      QueryResponse response = client.query(builder);
      if (response.getStatusCode() != 200) {
        return Status.ERROR;
      }
      if (response.getQueries().isEmpty()) {
        LOGGER.error("Found no queries for metric: {} for timestamp: {} to read.", metric, timestamp);
        return Status.NOT_FOUND;
      } else if (response.getQueries().size() > 1) {
        LOGGER.error("Found more than one queries for metric: {} for timestamp: {} to read. " +
            "This should normally not happen.", metric, timestamp);
      }

      long counter = response.getQueries().stream()
          .flatMap(q -> q.getResults().stream())
          .flatMap(result -> result.getDataPoints().stream())
          .filter(dataPoint -> dataPoint.getTimestamp() == timestamp)
          .count();
      if (counter == 0) {
        LOGGER.warn("Found no values for metric {} for timestamp {}.", metric, timestamp);
        return Status.NOT_FOUND;
      } else if (counter > 1) {
        LOGGER.warn("Found more than one value for metric {} for timestamp {}", metric, timestamp);
      }
      return Status.OK;
    } catch (URISyntaxException | IOException e) {
      LOGGER.error("Failed to process scan in metric {} for timestamp {}.{}", metric, timestamp, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    QueryBuilder builder = QueryBuilder.getInstance();
    QueryMetric qm = builder.setStart(new Timestamp(startTs))
        .setEnd(new Timestamp(endTs))
        .addMetric(metric);
    tags.forEach((name, values) -> qm.addTag(name, values.toArray(new String[0])));
    org.kairosdb.client.builder.TimeUnit tu = null;
    int newTimeValue = timeValue;
    // Can also support "weeks", "months", and "years", not used yet
    if (aggreg != AggregationOperation.NONE) {
      if (timeValue == 0) {
        newTimeValue = (int) (endTs - startTs);
        if (newTimeValue > TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)) {
          newTimeValue = (int) TimeUnit.DAYS.convert(endTs - startTs, TimeUnit.MILLISECONDS);
          tu = org.kairosdb.client.builder.TimeUnit.valueOf(TimeUnit.DAYS.name());
        } else if (newTimeValue > TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS)) {
          newTimeValue = (int) TimeUnit.HOURS.convert(endTs - startTs, TimeUnit.MILLISECONDS);
          tu = org.kairosdb.client.builder.TimeUnit.valueOf(TimeUnit.HOURS.name());
        } else if (newTimeValue > TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
          newTimeValue = (int) TimeUnit.MINUTES.convert(endTs - startTs, TimeUnit.MILLISECONDS);
          tu = org.kairosdb.client.builder.TimeUnit.valueOf(TimeUnit.MINUTES.name());
        } else if (newTimeValue > TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS)) {
          newTimeValue = (int) TimeUnit.SECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS);
          tu = org.kairosdb.client.builder.TimeUnit.valueOf(TimeUnit.SECONDS.name());
        } else {
          tu = org.kairosdb.client.builder.TimeUnit.valueOf(TimeUnit.MILLISECONDS.name());
        }
      } else {
        tu = org.kairosdb.client.builder.TimeUnit.valueOf(timeUnit.name());
      }
    }
    switch (aggreg) {
    case AVERAGE:
      qm.addAggregator(AggregatorFactory.createAverageAggregator(newTimeValue, tu));
      break;
    case COUNT:
      qm.addAggregator(AggregatorFactory.createCountAggregator(newTimeValue, tu));
      break;
    case SUM:
      qm.addAggregator(AggregatorFactory.createSumAggregator(newTimeValue, tu));
      break;
    default:
      // shut checkstyle up...
      break;
    }

    try {
      if (test) {
        return Status.OK;
      }
      QueryResponse response = client.query(builder);
      if (response.getStatusCode() != 200) {
        return Status.ERROR;
      }
      long counter = response.getQueries().stream()
          .flatMap(query -> query.getResults().stream())
          .mapToLong(result -> result.getDataPoints().size())
          .sum();
      return counter == 0 ? Status.NOT_FOUND : Status.OK;
    } catch (URISyntaxException | IOException e) {
      LOGGER.error("Failed to process scan for metric {} for timestamp-range: {}->{}.{}", metric, startTs, endTs, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    MetricBuilder builder = MetricBuilder.getInstance();
    Metric met = builder.addMetric(metric);
    tags.forEach((key, tag) -> met.addTag(key, tag.toString()));
    // cast is required for overload resolution to work properly
    met.addDataPoint((long) timestamp, value);
    try {
      if (test) {
        return Status.OK;
      }
      Response response = client.pushMetrics(builder);
      return response.getStatusCode() != 204 ? Status.ERROR : Status.OK;
    } catch (URISyntaxException | IOException e) {
      LOGGER.error("Failed to process insert to metric {}. {}", metric, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

