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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.*;

// Need to use Json 20140107, newer versions drop an error "Unsupported major.minor version 52.0"

/**
 * OpenTSDB client for YCSB framework.
 * OpenTSDB does not support one Bucket for avg/sum/count -> using interval size as Bucket
 */
public class OpenTSDBClient extends TimeseriesDB {
  // Configuration retrieval
  private static final String PROPERTY_IP = "ip";
  private static final String PROPERTY_PORT = "port";

  private static final String PROPERTY_QUERY_URL = "queryUrl";
  private static final String PROPERTY_QUERY_URL_DEFAULT = "/api/query";

  private static final String PROPERTY_PUT_URL = "putUrl";
  private static final String PROPERTY_PUT_URL_DEFAULT = "/api/put";

  private static final String PROPERTY_FILTER_FOR_TAGS = "filterForTags";
  private static final String PROPERTY_FILTER_FOR_TAGS_DEFAULT = "true";

  private static final String PROPERTY_USE_COUNT = "useCount";
  private static final String PROPERTY_USE_COUNT_DEFAULT = "true";

  private static final String PROPERTY_USE_MILLIS = "useMs";
  private static final String PROPERTY_USE_MILLIS_DEFAULT = "true";

  // configuration containers
  private URL urlQuery;
  private URL urlPut;
  private String ip;
  private int port;
  /**
   * Versions above OpenTSDB 2.2 (included) can use this; Untested! Defaults to true
   */
  private boolean filterForTags;
  /**
   * Versions above OpenTSDB 2.2 (included) have Count(), otherwise min is used. Defaults to true.
   */
  private boolean useCount;
  /**
   * Millisecond or Second resolution. Defaults to true -> Millisecond resolution.
   */
  private boolean useMs;

  // internal containers for implementation details
  private CloseableHttpClient client;
  // TODO: expose as configuration??
  private int retries = 3;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    if (!getProperties().containsKey("port") && !test) {
      throw new DBException("No port given, abort.");
    }
    if (!getProperties().containsKey("ip") && !test) {
      throw new DBException("No ip given, abort.");
    }
    ip = getProperties().getProperty(PROPERTY_IP, ip);
    port = Integer.parseInt(getProperties().getProperty(PROPERTY_PORT, String.valueOf(port)));

    filterForTags = Boolean.parseBoolean(getProperties().
        getProperty(PROPERTY_FILTER_FOR_TAGS, PROPERTY_FILTER_FOR_TAGS_DEFAULT));
    useCount = Boolean.parseBoolean(getProperties().getProperty(PROPERTY_USE_COUNT, PROPERTY_USE_COUNT_DEFAULT));
    useMs = Boolean.parseBoolean(getProperties().getProperty(PROPERTY_USE_MILLIS, PROPERTY_USE_MILLIS_DEFAULT));

    try {
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      String queryURL = getProperties().getProperty(PROPERTY_QUERY_URL, PROPERTY_QUERY_URL_DEFAULT);
      String putURL = getProperties().getProperty(PROPERTY_PUT_URL, PROPERTY_PUT_URL_DEFAULT);
      urlQuery = new URL("http", ip, port, queryURL);
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
      urlPut = new URL("http", ip, port, putURL);
      if (debug) {
        System.out.println("URL: " + urlPut);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }

    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
      }
    }
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
          System.err.println("WARNING: Query returned HTTP Status 301");
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
        client.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("")) {
      return Status.BAD_REQUEST;
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
    }
    // Problem: You cant ask for a timestamp at TS=x, you need to give a range.
    // So: Begin: timestamp, End: timestamp + 1 ms
    // We may get more than that, but we just take the right one
    // There could also be more of them, so count
    int counter = 0;
    JSONObject query = new JSONObject();
    query.put("start", timestamp);
    query.put("end", timestamp + 1);
    query.put("msResolution", this.useMs);

    JSONArray queryArray = new JSONArray();
    JSONObject queryParams = new JSONObject();
    queryParams.put("metric", metric);
    queryArray.put(queryParams);
    JSONArray queryFilters = new JSONArray();
    JSONObject queryTags = new JSONObject();
    queryParams.put("aggregator", "min");
    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      String tagValues = "";
      for (String tagValue : entry.getValue()) {
        tagValues += tagValue + "|";
      }
      tagValues = tagValues.substring(0, tagValues.length() - 1);
      if (filterForTags) {
        JSONObject tagFilter = new JSONObject();
        tagFilter.put("type", "literal_or");
        tagFilter.put("tagk", entry.getKey());
        tagFilter.put("groupBy", false);
        tagFilter.put("filter", tagValues);
        queryFilters.put(tagFilter);
      } else {
        queryTags.put(entry.getKey(), tagValues);
      }
    }
    if (filterForTags) {
      queryParams.put("filters", queryFilters);
    } else {
      queryParams.put("tags", queryTags);
    }
    query.put("queries", queryArray);
    if (debug) {
      System.out.println("Query String: " + query.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONArray jsonArr = runQuery(urlQuery, query.toString());
    if (jsonArr == null || jsonArr.length() == 0) {
      return Status.NOT_FOUND;
    }
    if (jsonArr.isNull(0)) {
      System.err.println("ERROR: Received empty Object for index 0 for '" + query.toString() + "'.");
      return Status.NOT_FOUND;
    }
    for (int i = 0; i < jsonArr.length(); i++) {
      if (!jsonArr.getJSONObject(i).has("dps")) {
        System.err.println("ERROR: jsonArr Index " + i + " has no 'dps' key.");
      } else {
        JSONObject jsonObj = (JSONObject) jsonArr.getJSONObject(i).get("dps");
        if (jsonObj.keySet().isEmpty()) {
          // Allowed to happen, no error message!
          return Status.OK;
        }
        for (Object obj : jsonObj.keySet()) {
          long objTS = Long.parseLong(obj.toString());
          if (objTS == timestamp) {
            counter++;
          }
        }
      }
    }
    if (debug) {
      System.err.println("Result JSON array: " + jsonArr);
    }
    if (counter == 0) {
      System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      System.err.println("ERROR: Found multiple values for metric: " + metric + " for timestamp: " + timestamp + ".");
    }
    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric.equals("")) {
      return Status.BAD_REQUEST;
    }
    if (startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    JSONObject query = new JSONObject();
    query.put("start", startTs);
    query.put("end", endTs);
    query.put("msResolution", this.useMs);
    String tu = "";
    if (aggreg != NONE) {
      if (timeValue != 0) {
        if (timeUnit == TimeUnit.MILLISECONDS) {
          tu = timeValue + "ms";
        } else if (timeUnit == TimeUnit.SECONDS) {
          tu = timeValue + "s";
        } else if (timeUnit == TimeUnit.MINUTES) {
          tu = timeValue + "m";
        } else if (timeUnit == TimeUnit.HOURS) {
          tu = timeValue + "h";
        } else if (timeUnit == TimeUnit.DAYS) {
          tu = timeValue + "d";
        } else {
          tu = TimeUnit.DAYS.convert(timeValue, timeUnit) + "d";
        }
      } else {
        long timeValueInMs = endTs - startTs;
        if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)) {
          tu = TimeUnit.DAYS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "d";
        } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS)) {
          tu = TimeUnit.HOURS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "h";
        } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
          tu = TimeUnit.MINUTES.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "m";
        } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS)) {
          tu = TimeUnit.SECONDS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "s";
        } else {
          tu = TimeUnit.MILLISECONDS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "ms";
        }
      }
    }
    JSONArray queryArray = new JSONArray();
    JSONObject queryParams = new JSONObject();
    queryParams.put("metric", metric);
    JSONArray queryFilters = new JSONArray();
    JSONObject queryTags = new JSONObject();
    // FIXME should we use downsampling passed by YCSB params here??
    if (aggreg == AVERAGE) {
      queryParams.put("aggregator", "avg");
      queryParams.put("downsample", tu + "-avg");
    } else if (aggreg == COUNT) {
      if (useCount) {
        queryParams.put("aggregator", "count");
        queryParams.put("downsample", tu + "-count");
      } else {
        queryParams.put("aggregator", "min");
        queryParams.put("downsample", tu + "-min");
      }
    } else if (aggreg == SUM) {
      queryParams.put("aggregator", "sum");
      queryParams.put("downsample", tu + "-sum");
    } else {
      // When scan do 1ms resolution, use min aggr.
      queryParams.put("aggregator", "min");
      queryParams.put("downsample", "1ms-min");
    }
    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      String tagValues = "";
      for (String tagValue : entry.getValue()) {
        tagValues += tagValue + "|";
      }
      tagValues = tagValues.substring(0, tagValues.length() - 1);
      if (filterForTags) {
        JSONObject tagFilter = new JSONObject();
        tagFilter.put("type", "literal_or");
        tagFilter.put("tagk", entry.getKey());
        tagFilter.put("groupBy", false);
        tagFilter.put("filter", tagValues);

        queryFilters.put(tagFilter);
      } else {
        queryTags.put(entry.getKey(), tagValues);
      }
    }
    if (filterForTags) {
      queryParams.put("filters", queryFilters);
    } else {
      queryParams.put("tags", queryTags);
    }
    queryArray.put(queryParams);
    query.put("queries", queryArray);
    if (debug) {
      System.out.println("Query String: " + query.toString());
    }
    if (test) {
      return Status.OK;
    }
    JSONArray jsonArr = runQuery(urlQuery, query.toString());
    if (jsonArr == null || jsonArr.length() == 0) {
      // is allowed
      return Status.NOT_FOUND;
    }
    if (jsonArr.isNull(0)) {
      System.err.println("ERROR: Received empty Object for index 0 for '" + query.toString() + "'.");
      return Status.NOT_FOUND;
    }
    for (int i = 0; i < jsonArr.length(); i++) {
      if (!jsonArr.getJSONObject(i).has("dps")) {
        System.err.println("ERROR: jsonArr Index " + i + " has no 'dps' key.");
      }
    }
    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("")) {
      return Status.BAD_REQUEST;
    }
    if (timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      JSONObject query = new JSONObject();
      query.put("timestamp", timestamp);
      query.put("metric", metric);
      query.put("value", value);
      JSONObject queryTags = new JSONObject();
      for (Map.Entry entry : tags.entrySet()) {
        queryTags.put(entry.getKey().toString(), entry.getValue().toString());
      }
      query.put("tags", queryTags);
      if (debug) {
        System.out.println("Input Query String: " + query.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONArray jsonArr = runQuery(urlPut, query.toString());
      if (debug) {
        System.err.println("jsonArr: " + jsonArr);
      }
      if (jsonArr == null) {
        System.err.println("ERROR: Error in processing insert to metric: " + metric);
        return Status.ERROR;
      }
      return Status.OK;

    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Not implemented in the Query API for opentsdb.
   * Can be achieved through the command-line tools in the distribution, but those are out of scope for the connector.
   */
  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

