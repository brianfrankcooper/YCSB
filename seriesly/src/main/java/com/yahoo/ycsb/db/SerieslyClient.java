/*
 * Copyright (c) 2015 - 2018 Michael Zimmermann, Andreas Bader, 2018 YCSB Contributors All rights reserved.
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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Seriesly client for YCSB framework. It's possible to store tags, as seriesly
 * is schemaless. But the filtering in seriesly doesn't work as the framework
 * expects it.
 *
 * @author Michael Zimmermann
 */
public class SerieslyClient extends TimeseriesDB {
  private static final String DATABASE_NAME = "TestDB";
  private static final String METRIC_FIELD_NAME = "metric";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String TAGS_FIELD_NAME = "tags";

  private static final String IP_PROPERTY = "ip";
  private static final String IP_PROPERTY_DEFAULT = "localhost";

  private static final String PORT_PROPERTY = "port";
  private static final int PORT_PROPERTY_DEFAULT = 3133;

  // configurable properties
  private String ip;
  private int port;

  // internally used fields
  private CloseableHttpClient client;
  private URL databaseURL = null;
  private int retries = 3;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  public void init() throws DBException {
    super.init();
    final Properties properties = getProperties();
    if (debug) {
      LOGGER.info("The following properties are given: ");
      for (String element : properties.stringPropertyNames()) {
        LOGGER.info("{}: {}", element, properties.getProperty(element));
      }
    }
    port = properties.containsKey(PORT_PROPERTY)
        ? Integer.parseInt(properties.getProperty(PORT_PROPERTY))
        : PORT_PROPERTY_DEFAULT;
    ip = properties.getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);

    try {
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        if (!properties.containsKey(PORT_PROPERTY)) {
          throwMissingProperty(PORT_PROPERTY);
        }
        if (!properties.containsKey(IP_PROPERTY)) {
          throwMissingProperty(IP_PROPERTY);
        }
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
    try {
      databaseURL = new URL("http", ip, port, "/" + DATABASE_NAME);
      if (debug) {
        LOGGER.info("URL: {}", databaseURL);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
    final Integer createDatabaseStatus = doPut(databaseURL);
    if (createDatabaseStatus == null || createDatabaseStatus != HttpURLConnection.HTTP_CREATED) {
      throw new DBException("Error creating the DB.");
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
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

  private Integer doPut(URL url) {
    HttpResponse response = null;
    HttpPut putMethod = new HttpPut(url.toString());
    try {
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          response = client.execute(putMethod);
          break;
        } catch (IOException e) {
          // no action necessary
        }
      }
      if (response == null) {
        LOGGER.error("Connection to {} failed {} times.", url, retries);
        return null;
      }
      putMethod.releaseConnection();
      return response.getStatusLine().getStatusCode();
    } catch (Exception e) {
      LOGGER.error("Failed to send PUT request to '{}' due to  {}", url, e);
      return null;
    } finally {
      putMethod.releaseConnection();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
  }

  private Integer doPost(URL url, String queryStr) {
    HttpResponse response = null;
    HttpPost postMethod = new HttpPost(url.toString());
    try {
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          response = client.execute(postMethod);
          break;
        } catch (IOException e) {
          // no action necessary
        }
      }
      if (response == null) {
        LOGGER.error("Connection to {} failed {} times.", url, retries);
        return null;
      }
      return response.getStatusLine().getStatusCode();
    } catch (Exception e) {
      LOGGER.error("Failed to query '{}' for '{}' due to {}", url, queryStr, e);
      return null;
    } finally {
      postMethod.releaseConnection();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
  }

  private JSONObject doGet(URL url) {
    HttpResponse response = null;
    HttpGet getMethod = new HttpGet(url.toString());
    getMethod.addHeader("accept", "application/json");
    try {
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          response = client.execute(getMethod);
          break;
        } catch (IOException e) {
          // no action necessary
        }
      }
      if (response == null) {
        LOGGER.error("Connection to {} failed {} times.", url, retries);
        return null;
      }
      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
        try (BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
          return new JSONObject(bis.lines().collect(Collectors.joining()));
        }
      }
      // if no exception was thrown, but we didn't get an answer
      return new JSONObject();
    } catch (Exception e) {
      LOGGER.error("Failed to query {} due to {}", url, e);
      return null;
    } finally {
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      getMethod.releaseConnection();
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    long timestampLong = timestamp;
    String urlStr = String.format("%s/_query?from=%s&to=%s&group=1&ptr=/%s&reducer=any&f=/%s&fv=%s", databaseURL,
        timestampLong, timestampLong, VALUE_FIELD_NAME, METRIC_FIELD_NAME, metric);
    final URL newQueryURL;
    try {
      newQueryURL = new URL(urlStr);
    } catch (MalformedURLException e) {
      LOGGER.error("A malformed URL was generated, {}", e);
      return Status.ERROR;
    }
    if (debug) {
      LOGGER.info("QueryURL: {}", newQueryURL);
    }
    if (test) {
      return Status.OK;
    }
    try {
      JSONObject jsonObject = doGet(newQueryURL);
      if (debug) {
        LOGGER.info("Answer: {}", jsonObject);
      }
      if (jsonObject == null) {
        return Status.ERROR;
      }
      JSONArray jsonArray = jsonObject.getJSONArray(Long.toString(timestampLong));
      if (jsonArray == null || jsonArray.length() < 1) {
        LOGGER.error("Found no values for metric {}", metric);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    String aggregation = "any";
    if (aggreg == AggregationOperation.AVERAGE) {
      aggregation = "avg";
    } else if (aggreg == AggregationOperation.COUNT) {
      aggregation = "count";
    } else if (aggreg == AggregationOperation.SUM) {
      aggregation = "sum";
    }

    Integer grouping;
    if (timeUnit == TimeUnit.MILLISECONDS) {
      grouping = 1;
    } else if (timeUnit == TimeUnit.SECONDS) {
      grouping = 1000;
    } else if (timeUnit == TimeUnit.MINUTES) {
      grouping = 60000;
    } else if (timeUnit == TimeUnit.HOURS) {
      grouping = 3600000;
    } else if (timeUnit == TimeUnit.DAYS) {
      grouping = 86400000;
    } else {
      LOGGER.warn("Timeunit {} is not supported. Using milliseconds instead!", timeUnit);
      grouping = 1;
    }
    grouping = grouping * timeValue;
    String urlStr = String.format("%s/_query?from=%s&to=%s&group=%s&ptr=/%s&reducer=%s&f=/%s&fv=%s", databaseURL,
        startTs, endTs, grouping, VALUE_FIELD_NAME, aggregation, METRIC_FIELD_NAME, metric);
    URL newQueryURL;
    try {
      newQueryURL = new URL(urlStr);
    } catch (MalformedURLException e) {
      LOGGER.error("A malformed URL was generated, {}", e);
      return Status.ERROR;
    }
    if (debug) {
      LOGGER.info("QueryURL: {}", newQueryURL);
    }
    if (test) {
      return Status.OK;
    }
    JSONObject jsonObject = doGet(newQueryURL);
    if (debug) {
      LOGGER.info("Answer: {}", jsonObject);
    }
    if (jsonObject == null) {
      return Status.ERROR;
    }
    try {
      String[] elementNames = JSONObject.getNames(jsonObject);
      JSONArray jsonArray = jsonObject.getJSONArray(elementNames[0]);
      if (jsonArray == null || jsonArray.length() < 1) {
        LOGGER.warn("Found no values for metric {}", metric);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    JSONObject insertObject = new JSONObject();
    insertObject.put(METRIC_FIELD_NAME, metric);
    insertObject.put(VALUE_FIELD_NAME, value);
    if (tags != null && !tags.isEmpty()) {
      JSONObject tagsObject = new JSONObject();
      tags.forEach((tagName, tagValue) -> tagsObject.put(tagName, tagValue.toString()));
      insertObject.put(TAGS_FIELD_NAME, tagsObject);
    }

    String query = insertObject.toString();
    String urlStr = String.format("%s?ts=%s", databaseURL.toString(), timestamp);
    URL insertURL;
    try {
      insertURL = new URL(urlStr);
    } catch (MalformedURLException e) {
      LOGGER.error("A malformed URL was generated, {}", e);
      return Status.ERROR;
    }
    if (debug) {
      LOGGER.info("Insert measures String: {}", query);
      LOGGER.info("Insert measures URL: {}", insertURL);
    }
    if (test) {
      return Status.OK;
    }
    Integer statusCode = doPost(insertURL, query);
    if (debug) {
      LOGGER.info("StatusCode: {}", statusCode);
    }
    if (statusCode == null || statusCode != HttpURLConnection.HTTP_CREATED) {
      LOGGER.error("Failed to process insert to metric {}", metric);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
