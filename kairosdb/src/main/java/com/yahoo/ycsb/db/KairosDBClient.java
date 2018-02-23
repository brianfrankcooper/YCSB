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
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.Result;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * KairosDB client for YCSB framework.
 */
public class KairosDBClient extends TimeseriesDB {

  private HttpClient client = null;
  private String ip = "localhost";
  private int port = 8080;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("ERROR: No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("ERROR: No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);
    } catch (Exception e) {
      throw new DBException(e);
    }
    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
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
    super.cleanup();
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
    int counter = 0;
    long timestampLong = timestamp;
    QueryBuilder builder = QueryBuilder.getInstance();
    QueryMetric qm = builder.setStart(new Timestamp(timestamp))
        .setEnd(new Timestamp(timestampLong + 1))
        .addMetric(metric);
    for (String tag : tags.keySet()) {
      List<String> tagnames = tags.get(tag);
      String[] tnArr = new String[tagnames.size()];
      tnArr = tagnames.toArray(tnArr);
      qm.addTag(tag, tnArr);
    }
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
        System.err.printf("ERROR: Found no queries for metric: %s for timestamp: %d to read.%n", metric, timestamp);
        return Status.NOT_FOUND;
      } else if (response.getQueries().size() > 1) {
        System.err.printf("ERROR: Found more than one queries for metric: %s for timestamp: %d to read. " +
            "This should normally not happen.%n", metric, timestamp);
      }

      for (Query query : response.getQueries()) {
        List<Result> resList = query.getResults();
        if (resList.isEmpty()) {
          return Status.NOT_FOUND;
        }
        for (Result result : resList) {
          List<DataPoint> dataPoints = result.getDataPoints();
          if (dataPoints.isEmpty()) {
            return Status.NOT_FOUND;
          }
          for (DataPoint dp : dataPoints) {
            if (dp.getTimestamp() == timestampLong) {
              counter++;
            }
          }
        }
      }
    } catch (URISyntaxException e) {
      System.err.printf("ERROR: Error in processing scan for metric: %s for timestamp: %d.%s%n",
          metric, timestamp, e);
      e.printStackTrace();
      return Status.ERROR;
    } catch (IOException e) {
      System.err.printf("ERROR: Error in processing scan for metric: %s  for timestamp: %d.%s%n",
          metric, timestamp, e);
      e.printStackTrace();
      return Status.ERROR;
    }

    if (counter == 0) {
      System.err.printf("ERROR: Found no values for metric: %s for timestamp: %d.%n", metric, timestamp);
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      System.err.printf("ERROR: Found more than one value for metric: %s for timestamp: %d.%n", metric, timestamp);
    }
    return Status.OK;
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
    for (String tag : tags.keySet()) {
      List<String> tagnames = tags.get(tag);
      String[] tnArr = new String[tagnames.size()];
      tnArr = tagnames.toArray(tnArr);
      qm.addTag(tag, tnArr);
    }
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
    if (aggreg == AggregationOperation.AVERAGE) {
      qm.addAggregator(AggregatorFactory.createAverageAggregator(newTimeValue, tu));
    } else if (aggreg == AggregationOperation.COUNT) {
      qm.addAggregator(AggregatorFactory.createCountAggregator(newTimeValue, tu));
    } else if (aggreg == AggregationOperation.SUM) {
      qm.addAggregator(AggregatorFactory.createSumAggregator(newTimeValue, tu));
    }
    try {
      if (test) {
        return Status.OK;
      }
      QueryResponse response = client.query(builder);
      if (response.getStatusCode() != 200) {
        return Status.ERROR;
      }
      if (response.getQueries().isEmpty()) {
        return Status.NOT_FOUND;
      }
      for (Query query : response.getQueries()) {
        List<Result> resList = query.getResults();
        if (resList.isEmpty()) {
          return Status.NOT_FOUND;
        }
        for (Result result : resList) {
          List<DataPoint> dataPoints = result.getDataPoints();
          if (dataPoints.isEmpty()) {
            return Status.NOT_FOUND;
          }
        }
      }
    } catch (URISyntaxException e) {
      System.err.printf("ERROR: Error in processing scan for metric: %s for timestamprange: %d->%d.%s%n",
          metric, startTs, endTs, e);
      e.printStackTrace();
      return Status.ERROR;
    } catch (IOException e) {
      System.err.printf("ERROR: Error in processing scan for metric: %s  for timestamprange: %d->%d.%s%n",
          metric, startTs, endTs, e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    MetricBuilder builder = MetricBuilder.getInstance();
    Metric met = builder.addMetric(metric);

    for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
      met.addTag(entry.getKey(), entry.getValue().toString());
    }
    // cast is required for overload resolution to work properly
    met.addDataPoint((long)timestamp, value);
    try {
      if (test) {
        return Status.OK;
      }
      Response response = client.pushMetrics(builder);
      if (response.getStatusCode() != 204) {
        return Status.ERROR;
      } else {
        return Status.OK;
      }
    } catch (URISyntaxException e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    } catch (IOException e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

