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

import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.finagle.Service;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.*;

/**
 * Druid Client for YCSB framework.
 * Can't work with Timestamps in the past
 * even with windowPeriod and segmentGranularity of 1 Year
 * Solution: Move the workload timerange to actual time
 * Needs a REST Client for Querys and a Finagle/Tranquility Service for Ingestion (Insert)
 * Druid supports the following granularities:
 * <ul>
 * <li>none (ms when ms is ingested)</li>
 * <li>minute</li>
 * <li>fifteen_minute</li>
 * <li>thirty_minute</li>
 * <li>hour</li>
 * <li>day</li>
 * <li>all (single bucket)</li>
 * </ul>
 */
public class DruidClient extends TimeseriesDB {

  private String zookeeperIP = "localhost";
  private String zookeeperPort = "2181";
  private String indexService = "overlord";
  private String firehosePattern = "druid:firehose:%s";
  private String discoveryPath = "/druid/discovery";
  private String dataSource = "usermetric";
  private String queryIP = "localhost"; // normally broker node,but historical/realtime is possible
  private String queryPort = "8090"; // normally broker node,but historical/realtime is possible
  private String queryURL = "/druid/v2/?pretty";
  private int retries = 3;

  private URL urlQuery = null;
  private long insertStart = 0; // Workload
  private long insertEnd = 0; // Workload
  private int timeResolution = 0; // Workload
  private long realInsertStart = 0; // Druid
  private long realInsertEnd = 0; // Druid

  private Period windowPeriod = new Period().withDays(2);
  private Granularity segmentGranularity = Granularity.MONTH;
  // Millisecond Querys
  private QueryGranularity queryGranularity = QueryGranularities.NONE;
  // does not help for first task, but spawns task for second segment 10 minutes earlier,
  // see: https://groups.google.com/forum/#!topic/druid-user/UT5JNSZqAuk
  private Period warmingPeriod = new Period().withMinutes(10);
  private int partitions = 1;
  private int replicants = 1;

  // if false do longSum, which should be faster (non floating point),
  // see http://druid.io/docs/latest/querying/aggregations.html
  private boolean doubleSum = true;
  private List<String> dimensions = new ArrayList<>();
  private CuratorFramework curator;
  private CloseableHttpClient client;
  private final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto", null);
  private Tranquilizer<Map<String, Object>> druidService;

  private String phase;
  private static final String PHASE_PROPERTY = "phase"; // See Client.java
  private static final String TAG_PREFIX_PROPERTY = "tagprefix"; // see CoreWorkload.java
  private static final String TAG_PREFIX_PROPERTY_DEFAULT = "TAG"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY = "tagcount"; // see CoreWorkload.java
  private static final String TAG_COUNT_PROPERTY_DEFAULT = "3"; // see CoreWorkload.java
  private static final String METRICNAME_PROPERTY = "metric"; // see CoreWorkload.java
  private String timestampFile = "timestamps.txt";

  /**
   * @inheritDoc
   */
  @Override
  public void init() throws DBException {
    super.init();
    readProperties();
    try {
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }


      curator = CuratorFrameworkFactory
          .builder()
          .connectString(String.format("%s:%s", zookeeperIP, zookeeperPort))
          .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
          .build();
      if (!test) {
        curator.start();
        druidService = DruidBeams
            .builder((Timestamper<Map<String, Object>>) theMap -> new DateTime(theMap.get("timestamp")))
            .curator(curator)
            .discoveryPath(discoveryPath)
            .location(
                DruidLocation.create(
                    indexService,
                    firehosePattern,
                    dataSource
                )
            )
            .timestampSpec(timestampSpec)
            .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), new ArrayList<>(), queryGranularity))
            .tuning(
                ClusteredBeamTuning
                    .builder()
                    .segmentGranularity(this.segmentGranularity)
                    .windowPeriod(this.windowPeriod)
                    .partitions(this.replicants)
                    .replicants(this.partitions)
                    .warmingPeriod(this.warmingPeriod)
                    .build()
            )
            .buildTranquilizer();
        RequestConfig requestConfig = RequestConfig.custom().build();
        if (!test) {
          client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        }
      }
      urlQuery = new URL("http", queryIP, Integer.valueOf(queryPort), queryURL);
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
      if (phase.equals("load")) {
        // write timestamps
        String actTime = String.valueOf(System.currentTimeMillis());
        realInsertStart = Long.valueOf(actTime.substring(0, actTime.length() - 3) + "000");
        realInsertEnd = realInsertStart + Math.abs(insertStart - insertEnd);
        File file = new File(timestampFile);
        if (file.isDirectory()) {
          throw new DBException(String.format("'%s' is a directory. Please specify a file.", timestampFile));
        }
        if (file.exists()) {
          file.delete();
        }
        FileOutputStream fileOut = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fileOut);
        oos.writeObject(realInsertStart);
        oos.writeObject(realInsertEnd);
        oos.flush();
        oos.close();
        fileOut.flush();
        fileOut.close();
      } else if (phase.equals("run")) {
        // read timestamps
        File file = new File(timestampFile);
        if (file.exists() && file.canRead() && file.isFile()) {
          FileInputStream fileIn = new FileInputStream(timestampFile);
          ObjectInputStream ois = new ObjectInputStream(fileIn);
          realInsertStart = (long) ois.readObject();
          realInsertEnd = (long) ois.readObject();
          ois.close();
          fileIn.close();
          file.delete();
        } else {
          throw new DBException(String.format("'%s' is not existing,not readable or not a file.", timestampFile));
        }

      } else {
        throw new DBException(String.format("Unknown Phase: '%s'", phase));
      }
      if (debug) {
        System.out.println(String.format("Workload Timerange: %s (%s) %s (%s)",
            insertStart, new DateTime(insertStart),
            insertEnd, new DateTime(insertEnd)));
        System.out.println(String.format("Druid Timerange: %s (%s) %s (%s)",
            realInsertStart, new DateTime(realInsertStart),
            realInsertEnd, new DateTime(realInsertEnd)));
      }
      if (!test && !this.druidService.isAvailable()) {
        throw new DBException("ERROR: DruidService is not available!");
      }
      // we need one insert, because this first insert takes a lot time
      // because of no worker available etcpp.
      // insert 10ms before time range, should not matter
      if (debug) {
        System.out.println("Doing Init Insert now.");
      }
      Status res = this.insert(dataSource, insertStart - 10, 1, new HashMap<>());
      if (debug) {
        System.out.println(String.format("Init Insert returned %s.", res));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private void readProperties() throws DBException {
    if (!getProperties().containsKey(PHASE_PROPERTY)) {
      throw new DBException(String.format("No %s given, abort.", PHASE_PROPERTY));
    }
    phase = getProperties().getProperty(PHASE_PROPERTY, "");
    dataSource = getProperties().getProperty(METRICNAME_PROPERTY, dataSource);
    int tagCount = Integer.valueOf(getProperties().getProperty(TAG_COUNT_PROPERTY, TAG_COUNT_PROPERTY_DEFAULT));
    String tagPrefix = getProperties().getProperty(TAG_PREFIX_PROPERTY, TAG_PREFIX_PROPERTY_DEFAULT);
    dimensions.add("value");
    for (int i = 0; i < tagCount; i++) {
      dimensions.add(tagPrefix + i);
    }
    timestampFile = getProperties().getProperty("timestampfile", timestampFile);
    insertStart = Long.parseLong(getProperties().getProperty("insertstart", "0"));
    if (insertStart == 0) {
      throw new DBException("insertstart must not be 0, is it not set?");
    }
    insertEnd = Long.parseLong(getProperties().getProperty("insertend", "0"));
    if (insertEnd == 0) {
      throw new DBException("insertend must not be 0, is it not set?");
    }
    timeResolution = Integer.parseInt(getProperties().getProperty("timeresolution", "0"));
    if (timeResolution == 0) {
      throw new DBException("timeresolution must not be 0, is it not set?");
    }
    test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
    doubleSum = Boolean.parseBoolean(getProperties().getProperty("doublesum", Boolean.toString(doubleSum)));
    if (!getProperties().containsKey("zookeeperip") && !test) {
      throw new DBException("No zookeeperip given, abort.");
    }
    if (!getProperties().containsKey("queryip") && !test) {
      throw new DBException("No queryip given, abort.");
    }
    partitions = Integer.parseInt(getProperties().getProperty("partitions", String.valueOf(partitions)));
    replicants = Integer.parseInt(getProperties().getProperty("replicants", String.valueOf(replicants)));
    zookeeperIP = getProperties().getProperty("zookeeperip", zookeeperIP);
    zookeeperPort = getProperties().getProperty("zookeeperport", zookeeperPort);
    queryIP = getProperties().getProperty("queryip", queryIP);
    queryPort = getProperties().getProperty("queryport", queryPort);
    indexService = getProperties().getProperty("indexservice", indexService);
    firehosePattern = getProperties().getProperty("firehosepattern", firehosePattern);
    discoveryPath = getProperties().getProperty("discoverypath", discoveryPath);
    firehosePattern = getProperties().getProperty("firehosepattern", firehosePattern);
    dataSource = getProperties().getProperty("datasource", dataSource);
  }

  private JSONArray runQuery(URL url, String queryStr) {
    JSONArray jsonArr = new JSONArray();
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      postMethod.addHeader("accept", "application/json");
      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(postMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            postMethod.releaseConnection();
            return null;
          }
        }
      }
      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
          System.err.println("WARNING: Query returned 301");
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
          BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          StringBuilder builder = new StringBuilder();
          String line;
          while ((line = bis.readLine()) != null) {
            builder.append(line);
          }
          jsonArr = new JSONArray(builder.toString());
        }
        EntityUtils.consumeQuietly(response.getEntity());
        postMethod.releaseConnection();
      }
    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return jsonArr;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void cleanup() throws DBException {
    try {
      if (!test) {
        Await.result(druidService.close());
        curator.close();
        client.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.cleanup();
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      int counter = 0;
      JSONObject query = new JSONObject();
      query.put("queryType", "select");
      query.put("dataSource", metric);
      query.put("dimensions", new JSONArray());
      query.put("metrics", new JSONArray());
      query.put("granularity", "all");
      if (!tags.isEmpty()) {
        JSONObject andFilter = new JSONObject();
        andFilter.put("type", "and");
        JSONArray andArray = new JSONArray();
        for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
          JSONObject orFilter = new JSONObject();
          JSONArray orArray = new JSONArray();
          orFilter.put("type", "or");
          for (String tagValue : entry.getValue()) {
            JSONObject selectorFilter = new JSONObject();
            selectorFilter.put("type", "selector");
            selectorFilter.put("dimension", entry.getKey().toString());
            selectorFilter.put("value", tagValue);
            orArray.put(selectorFilter);
          }
          orFilter.put("fields", orArray);
          andArray.put(orFilter);
        }
        andFilter.put("fields", andArray);
        query.put("filter", andFilter);
      }
      JSONArray dateArray = new JSONArray();
      // calculate druid timestamps from workload timestamps
      long realTimestamp = this.realInsertStart + Math.abs(timestamp - this.insertStart);
      dateArray.put(String.format("%s/%s",
          new DateTime(realTimestamp),
          new DateTime(realTimestamp + timeResolution)));
      query.put("intervals", dateArray);
      JSONObject pagingSpec = new JSONObject();
      pagingSpec.put("pagingIdentifiers", new JSONObject());
      pagingSpec.put("threshold", 1);
      query.put("pagingSpec", pagingSpec);
      if (debug) {
        System.out.println("Input Query String: " + query.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONArray jsonArr = runQuery(urlQuery, query.toString());
      if (jsonArr == null || jsonArr.length() == 0) {
        return Status.NOT_FOUND;
      }

      for (int i = 0; i < jsonArr.length(); i++) {
        if (!jsonArr.getJSONObject(i).has("timestamp")) {
          System.err.println("ERROR: jsonArr Index " + i + " has no 'timestamp' key.");
          continue;
        } else if (!jsonArr.getJSONObject(i).has("result")) {
          System.err.println("ERROR: jsonArr Index " + i + " has no 'result' key.");
          continue;
        }
        DateTime ts = new DateTime(jsonArr.getJSONObject(i).get("timestamp"));
        JSONObject result = (JSONObject) jsonArr.getJSONObject(i).get("result");
        if (!result.has("events")) {
          System.err.println("ERROR: jsonArr Index " + i + " has no 'result->events' key.");
        } else {
          JSONArray events = result.getJSONArray("events");
          if (events == null || events.length() == 0) {
            System.err.println("ERROR: jsonArr Index " + i + " has no 'result->events'.");
          } else {
            for (int j = 0; j < events.length(); j++) {
              JSONObject event = events.getJSONObject(j);
              if (event == null || event.length() == 0 || !event.has("event")) {
                System.err.println("ERROR: jsonArr Index " + i + " has no 'result->events->event' at index " + j + ".");
              } else {
                JSONObject eventEntry = event.getJSONObject("event");
                if (eventEntry.has("timestamp")) {
                  DateTime ts2 = new DateTime(eventEntry.get("timestamp"));
                  if (ts.getMillis() == ts2.getMillis() && ts.getMillis() == realTimestamp) {
                    counter++;
                  }
                }
              }
            }
          }
        }

      }
      if (debug) {
        System.err.println("jsonArr: " + jsonArr);
      }
      if (counter == 0) {
        System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
        return Status.NOT_FOUND;
      } else if (counter > 1) {
        System.err.println("ERROR: Found multiple values for metric: " + metric + " for timestamp: " + timestamp + ".");
      }
    } catch (Exception e) {
      System.err.println("ERROR: Error while processing READ for metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    try {
      JSONObject query = new JSONObject();
      if (aggreg != NONE) {
        putAggregationProperties(query, aggreg);
      } else {
        query.put("queryType", "select");
        query.put("dimensions", new JSONArray());
        query.put("metrics", new JSONArray());
        JSONObject pagingSpec = new JSONObject();
        pagingSpec.put("pagingIdentifiers", new JSONObject());
        pagingSpec.put("threshold", 1);
        query.put("pagingSpec", pagingSpec);
      }
      query.put("dataSource", metric);
      if (aggreg != NONE) {
        if (timeValue != 0) {
          long granularity = TimeUnit.MILLISECONDS.convert(timeValue, timeUnit);
          if (granularity == TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
            query.put("granularity", "minute");
          } else if (granularity == TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES)) {
            query.put("granularity", "fifteen_minute");
          } else if (granularity == TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES)) {
            query.put("granularity", "thirty_minute");
          } else if (granularity == TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES)) {
            query.put("granularity", "hour");
          } else if (granularity == TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)) {
            query.put("granularity", "day");
          } else if (granularity == 1) {
            System.err.println("WARNING: Using granularity = 1 ms for Druid is not advisable and will " +
                "probably lead to problems (Search for killed Java processes due to memory). " +
                "See http://druid.io/docs/latest/querying/granularities.html.");
            query.put("granularity", "none");
          } else {
            System.err.print("WARNING: Druid only supports 1 minute, 15 minutes, 30 minutes, 1 hour, 1 day, " +
                "none (= ingested resultion = ms?), all (one bucket) as granularity. Using full granularity.");
          }
        } else {
          query.put("granularity", "all");
        }
      } else {
        // WARNING: Using granularity = 1 ms for Druid is not advisable and will probably lead to problems
        // (Search for killed Java processes due to memory).
        // See http://druid.io/docs/latest/querying/granularities.html
        // Also a Select Query needs no "none" = 1 ms granularity,
        // see http://druid.io/docs/latest/development/select-query.html
        // it is okay to return every existing value in one big bucket, as long as all values are delivered back
        query.put("granularity", "all");

      }
      if (!tags.isEmpty()) {
        JSONObject andFilter = new JSONObject();
        andFilter.put("type", "and");
        JSONArray andArray = new JSONArray();
        for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
          JSONObject orFilter = new JSONObject();
          JSONArray orArray = new JSONArray();
          orFilter.put("type", "or");
          for (String tagValue : entry.getValue()) {
            JSONObject selectorFilter = new JSONObject();
            selectorFilter.put("type", "selector");
            selectorFilter.put("dimension", entry.getKey());
            selectorFilter.put("value", tagValue);
            orArray.put(selectorFilter);
          }
          orFilter.put("fields", orArray);
          andArray.put(orFilter);
        }
        andFilter.put("fields", andArray);
        query.put("filter", andFilter);
      }
      JSONArray dateArray = new JSONArray();
      // calculate druid timestamps from workload timestamps
      dateArray.put(String.format("%s/%s",
          new DateTime(this.realInsertStart + Math.abs(startTs - this.insertStart)),
          new DateTime(this.realInsertStart + Math.abs(endTs - this.insertStart))));
      query.put("intervals", dateArray);
      if (debug) {
        System.out.println("Input Query String: " + query.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONArray jsonArr = runQuery(urlQuery, query.toString());
      if (jsonArr == null || jsonArr.length() == 0) {
        return Status.NOT_FOUND;
      }

      for (int i = 0; i < jsonArr.length(); i++) {
        if (!jsonArr.getJSONObject(i).has("timestamp")) {
          return Status.ERROR;
        } else if (!jsonArr.getJSONObject(i).has("result")) {
          return Status.ERROR;
        } else {
          DateTime ts = new DateTime(jsonArr.getJSONObject(i).get("timestamp"));
          JSONObject result = (JSONObject) jsonArr.getJSONObject(i).get("result");
          if (aggreg == SUM) {
            if (!result.has("sumResult")) {
              return Status.ERROR;
            }
          } else if (aggreg == AVERAGE) {
            if (!result.has("averageResult")) {
              return Status.ERROR;
            }
          } else if (aggreg == COUNT) {
            if (!result.has("countResult")) {
              return Status.ERROR;
            }
          } else if (!result.has("events")) {
            return Status.ERROR;
          } else {
            JSONArray events = result.getJSONArray("events");
            if (events == null || events.length() == 0) {
              return Status.NOT_FOUND;
            }
          }
        }
      }
      if (debug) {
        System.err.println("jsonArr: " + jsonArr);
      }
    } catch (Exception e) {
      System.err.println("ERROR: Error while processing READ for metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void putAggregationProperties(JSONObject query, AggregationOperation aggreg) {
    query.put("queryType", "timeseries");
    JSONArray aggregatorArray = new JSONArray();
    if (aggreg == SUM || aggreg == AVERAGE) {
      JSONObject aggregator = new JSONObject();
      if (this.doubleSum) {
        aggregator.put("type", "doubleSum");
      } else {
        aggregator.put("type", "longSum");
      }
      aggregator.put("name", "sumResult");
      aggregator.put("fieldName", "value");
      aggregatorArray.put(aggregator);
    }

    if (aggreg == COUNT || aggreg == AVERAGE) {
      JSONObject aggregator = new JSONObject();
      aggregator.put("type", "count");
      aggregator.put("name", "countResult");
      aggregatorArray.put(aggregator);
    }
    query.put("aggregations", aggregatorArray);
    if (aggreg == AVERAGE) {
      JSONObject postAggregator = new JSONObject();
      postAggregator.put("type", "arithmetic");
      postAggregator.put("name", "averageResult");
      postAggregator.put("fn", "/");
      JSONArray fields = new JSONArray();
      JSONObject field1 = new JSONObject();
      field1.put("type", "fieldAccess");
      field1.put("name", "sumResult");
      field1.put("fieldName", "sumResult");
      fields.put(field1);
      JSONObject field2 = new JSONObject();
      field2.put("type", "fieldAccess");
      field2.put("name", "countResult");
      field2.put("fieldName", "countResult");
      fields.put(field2);
      postAggregator.put("fields", fields);
      JSONArray postAggregatorArray = new JSONArray();
      postAggregatorArray.put(postAggregator);
      query.put("postAggregations", postAggregatorArray);
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (!metric.equals(dataSource)) {
      System.err.println(String.format("WARNING: Metric and Datasource differ! Metric: %s, dataSource: %s.",
          metric, this.dataSource));
    }
    Map<String, Object> data = new HashMap<>();
    // Calculate special Timestamp in special druid timerange
    // (workload timerange shifted to actual time)
    data.put("timestamp", this.realInsertStart + (timestamp - this.insertStart));
    for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
      data.put(tag.getKey(), tag.getValue().toString());
    }
    data.put("value", value);
    List<Map<String, Object>> druidEvents = new ArrayList<>();
    druidEvents.add(data);
    final Future<Integer> numSentFuture = druidService.apply(druidEvents);
    try {
      final Integer numSent = Await.result(numSentFuture);
      if (debug) {
        System.out.println(String.format("Result (numSent) from Druid: %s", numSent));
      }
      if (numSent != 1) {
        return Status.UNEXPECTED_STATE;
      }
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

