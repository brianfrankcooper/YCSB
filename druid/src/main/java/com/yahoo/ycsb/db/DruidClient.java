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
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.CoreWorkload;
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
import scala.runtime.BoxedUnit;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.*;
import static java.nio.file.StandardOpenOption.*;

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
  // constants
  private static final String QUERY_ENDPOINT = "/druid/v2/?pretty";
  private static final String FIREHOSE_PATTERN = "druid:firehose:%s";

  // property-related constants
  private static final String ZOOKEEPER_IP_PROPERTY = "zookeeperIp";
  private static final String QUERY_IP_PROPERTY = "queryIp";

  // replace with CoreWorkload.INSERT_START_PROPERTY?
  private static final String INSERT_START_PROPERTY = "insertStart";
  private static final String INSERT_END_PROPERTY = "insertEnd";
  private static final String TIME_RESOLUTION_PROPERTY = "timeResolution";

  private static final String ZOOKEEPER_PORT_PROPERTY = "zookeeperPort";
  private static final String ZOOKEEPER_PORT_PROPERTY_DEFAULT = "2181";

  private static final String INDEX_SERVICE_PROPERTY = "indexService";
  private static final String INDEX_SERVICE_PROPERTY_DEFAULT = "overlord";

  private static final String SERVICE_DISCOVERY_PATH_PROPERTY = "serviceDiscoveryPath";
  private static final String SERVICE_DISCOVERY_PATH_PROPERTY_DEFAULT = "/druid/discovery";

  private static final String QUERY_PORT_PROPERTY = "queryPort";
  private static final String QUERY_PORT_PROPERTY_DEFAULT = "8090";

  private static final String PARTITIONS_PROPERTY = "partitions";
  private static final int PARTITIONS_PROPERTY_DEFAULT = 1;

  private static final String REPLICANTS_PROPERTY = "replicants";
  private static final int REPLICANTS_PROPERTY_DEFAULT = 1;

  private static final String TIMESTAMP_FILE_PROPERTY = "timestampFile";
  private static final String TIMESTAMP_FILE_PROPERTY_DEFAULT = "timestamps.txt";

  private static final String DOUBLE_SUM_PROPERTY = "doubleSum";
  private static final String DOUBLE_SUM_PROPERTY_DEFAULT = "true";

  // properties themselves
  private String zookeeperIP;
  private String zookeeperPort;
  private String indexService;
  private String discoveryPath;
  private String metricName;
  // usually the broker node, but it can directly point to a historical / realtime node as well
  private String queryIP;
  private String queryPort;

  private int partitions;
  private int replicants;

  // the following three are what the workload expects.
  // they must be transposed from [insertStart, insertEnd] into [realInsertStart, realInsertEnd]
  private long insertStart;
  private long insertEnd;
  // FIXME make obsolete by changing the timestamp schema to not use intervals
  private int timeResolution;

  private String timestampFile = "timestamps.txt";
  // if false do longSum, which should be faster (non floating point),
  // see http://druid.io/docs/latest/querying/aggregations.html
  private boolean doubleSum = true;

  // internal workings

  // are read from file / written to file during init()
  // see #insertStart
  private long realInsertStart = 0;
  private long realInsertEnd = 0;

  private URL urlQuery;
  private String timestampColumn = "timestamp";
  private int retries = 3;

  private Period windowPeriod = new Period().withDays(2);
  private Granularity segmentGranularity = Granularity.MONTH;
  // Millisecond Querys
  private QueryGranularity queryGranularity = QueryGranularities.NONE;
  // does not help for first task, but spawns task for second segment 10 minutes earlier,
  // see: https://groups.google.com/forum/#!topic/druid-user/UT5JNSZqAuk
  private Period warmingPeriod = new Period().withMinutes(10);

  private List<String> dimensions = new ArrayList<>();
  private CuratorFramework curator;
  private CloseableHttpClient client;
  private final TimestampSpec timestampSpec = new TimestampSpec(timestampColumn, "auto", null);
  private Tranquilizer<Map<String, Object>> druidService;

  private boolean isRun;

  private void readProperties() throws DBException {
    Properties properties = getProperties();
    if (!properties.containsKey(Client.DO_TRANSACTIONS_PROPERTY)) {
      throwMissingProperty(Client.DO_TRANSACTIONS_PROPERTY);
    }
    if (!properties.containsKey(ZOOKEEPER_IP_PROPERTY) && !test) {
      throwMissingProperty(ZOOKEEPER_IP_PROPERTY);
    }
    if (!properties.containsKey(QUERY_IP_PROPERTY) && !test) {
      throwMissingProperty(QUERY_IP_PROPERTY);
    }
    if (!properties.containsKey(INSERT_START_PROPERTY)) {
      throwMissingProperty(INSERT_START_PROPERTY);
    }
    if (!properties.containsKey(INSERT_END_PROPERTY)) {
      throwMissingProperty(INSERT_END_PROPERTY);
    }
    if (!properties.containsKey(TIME_RESOLUTION_PROPERTY)) {
      throwMissingProperty(TIME_RESOLUTION_PROPERTY);
    }

    dimensions.add("value");
    dimensions.addAll(Arrays.asList(getPossibleTagKeys(properties)));

    isRun = Boolean.valueOf(properties.getProperty(Client.DO_TRANSACTIONS_PROPERTY));
    insertStart = Long.parseLong(properties.getProperty(INSERT_START_PROPERTY));
    insertEnd = Long.parseLong(properties.getProperty(INSERT_END_PROPERTY));
    timeResolution = Integer.parseInt(properties.getProperty(TIME_RESOLUTION_PROPERTY));
    doubleSum = Boolean.parseBoolean(properties.getProperty(DOUBLE_SUM_PROPERTY, DOUBLE_SUM_PROPERTY_DEFAULT));
    timestampFile = properties.getProperty(TIMESTAMP_FILE_PROPERTY, TIMESTAMP_FILE_PROPERTY_DEFAULT);

    if (insertStart == 0) {
      throw new DBException(INSERT_START_PROPERTY + "must not be 0!");
    }
    if (insertEnd == 0) {
      throw new DBException(INSERT_END_PROPERTY + "must not be 0!");
    }
    if (timeResolution == 0) {
      throw new DBException(TIME_RESOLUTION_PROPERTY + " must not be 0!");
    }
    try {
      // TODO support https?
      urlQuery = new URL("http", queryIP, Integer.valueOf(queryPort), QUERY_ENDPOINT);
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }

    zookeeperIP = properties.getProperty(ZOOKEEPER_IP_PROPERTY);
    zookeeperPort = properties.getProperty(ZOOKEEPER_PORT_PROPERTY, ZOOKEEPER_PORT_PROPERTY_DEFAULT);
    indexService = properties.getProperty(INDEX_SERVICE_PROPERTY, INDEX_SERVICE_PROPERTY_DEFAULT);
    discoveryPath = properties.getProperty(SERVICE_DISCOVERY_PATH_PROPERTY, SERVICE_DISCOVERY_PATH_PROPERTY_DEFAULT);
    metricName = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    queryIP = properties.getProperty(QUERY_IP_PROPERTY);
    queryPort = properties.getProperty(QUERY_PORT_PROPERTY, QUERY_PORT_PROPERTY_DEFAULT);

    partitions = properties.containsKey(PARTITIONS_PROPERTY)
        ? Integer.parseInt(properties.getProperty(PARTITIONS_PROPERTY))
        : PARTITIONS_PROPERTY_DEFAULT;
    replicants = properties.containsKey(REPLICANTS_PROPERTY)
        ? Integer.parseInt(properties.getProperty(REPLICANTS_PROPERTY))
        : REPLICANTS_PROPERTY_DEFAULT;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void init() throws DBException {
    super.init();
    readProperties();

    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
      }
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
    }

    if (!test) {
      curator = CuratorFrameworkFactory
          .builder()
          .connectString(String.format("%s:%s", zookeeperIP, zookeeperPort))
          .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
          .build();
      curator.start();
      druidService = DruidBeams
          .builder((Timestamper<Map<String, Object>>) theMap -> new DateTime(theMap.get(timestampColumn)))
          .curator(curator)
          .discoveryPath(discoveryPath)
          .location(
              DruidLocation.create(
                  indexService,
                  FIREHOSE_PATTERN,
                  metricName
              )
          )
          .timestampSpec(timestampSpec)
          // FIXME Tags not used at all during the actual workload
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

    try {
      // Accesses the properties from client... possibly inline? then we don't need the field
      if (isRun) {
        readRealInsertTimestamps();
      } else {
        String actTime = String.valueOf(System.currentTimeMillis());
        realInsertStart = Long.valueOf(actTime.substring(0, actTime.length() - 3) + "000");
        realInsertEnd = realInsertStart + Math.abs(insertStart - insertEnd);
        writeRealInsertTimstamps();
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
        System.out.println("Starting initial insert.");
      }
      Status res = this.insert(metricName, insertStart - 10, 1, new HashMap<>());
      if (debug) {
        System.out.println(String.format("Initial insert returned Status %s.", res.getName()));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private void readRealInsertTimestamps() throws IOException, DBException {
    Path timestamps = Paths.get(timestampFile);
    if (Files.isReadable(timestamps) && Files.isRegularFile(timestamps)) {
      // FIXME reconsider DELETE_ON_CLOSE. It breaks load-once-run-many
      try (InputStream fileIn = Files.newInputStream(timestamps, DELETE_ON_CLOSE, READ);
           ObjectInputStream ois = new ObjectInputStream(fileIn)) {
        realInsertStart = ois.readLong();
        realInsertEnd = ois.readLong();
      }
    } else {
      throw new DBException(String.format("'%s' is not existing,not readable or not a file.", timestampFile));
    }
  }

  private void writeRealInsertTimstamps() throws DBException, IOException {
    Path timestamps = Paths.get(timestampFile);
    if (Files.isDirectory(timestamps)) {
      throw new DBException(String.format("'%s' is a directory. Please specify a file.", timestampFile));
    }
    try (OutputStream fileOut = Files.newOutputStream(timestamps, WRITE, CREATE, TRUNCATE_EXISTING);
         ObjectOutputStream oos = new ObjectOutputStream(fileOut)) {
      oos.writeLong(realInsertStart);
      oos.writeLong(realInsertEnd);
    }
  }

  private JSONArray runQuery(URL url, String queryStr) {
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
          // better be sure to close this ...
          try (BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            return new JSONArray(bis.lines().collect(Collectors.joining()));
          }
        }
        EntityUtils.consumeQuietly(response.getEntity());
        postMethod.releaseConnection();
      }
    } catch (Exception e) {
      System.err.println("ERROR: Error while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
    return null;
  }

  /**
   * @inheritDoc
   */
  @Override
  public void cleanup() throws DBException {
    if (!test) {
      try {
        Await.result(druidService.close());
        curator.close();
        client.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
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
      // FIXME check whether we need to add dimensions here
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
        if (!jsonArr.getJSONObject(i).has(timestampColumn)) {
          System.err.println("ERROR: jsonArr Index " + i + " has no 'timestamp' key.");
          continue;
        } else if (!jsonArr.getJSONObject(i).has("result")) {
          System.err.println("ERROR: jsonArr Index " + i + " has no 'result' key.");
          continue;
        }
        DateTime ts = new DateTime(jsonArr.getJSONObject(i).get(timestampColumn));
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
                if (eventEntry.has(timestampColumn)) {
                  DateTime ts2 = new DateTime(eventEntry.get(timestampColumn));
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
        // FIXME check whether we need to add dimensions here
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
        if (!jsonArr.getJSONObject(i).has(timestampColumn)) {
          return Status.ERROR;
        } else if (!jsonArr.getJSONObject(i).has("result")) {
          return Status.ERROR;
        } else {
          DateTime ts = new DateTime(jsonArr.getJSONObject(i).get(timestampColumn));
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
    if (!metric.equals(metricName)) {
      System.err.println(String.format("WARNING: Workload Metric and Database Metric differ! " +
              "Workload: %s, Database: %s.", metric, this.metricName));
    }
    Map<String, Object> data = new HashMap<>();
    // Calculate special Timestamp in special druid timerange
    // (workload timerange shifted to actual time)
    data.put(timestampColumn, this.realInsertStart + (timestamp - this.insertStart));
    for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
      data.put(tag.getKey(), tag.getValue().toString());
    }
    data.put("value", value);
    // BoxedUnit is equivalent to Unit, which is Void in java
    final Future<BoxedUnit> numSentFuture = druidService.apply(data);
    try {
      Await.result(numSentFuture);
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

