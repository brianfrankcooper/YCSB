/*
 * Copyright (c) 2015 - 2018 Andreas Bader, Vincenz Pauly 2018 YCSB Contributors All rights reserved.
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
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Prometheus client for YCSB framework.
 */
public class PrometheusClient extends TimeseriesDB {

  private static final DateFormat RFC_3339_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  private static final Pattern METRIC_REGEX = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");

  private static final String PUT_ENDPOINT = "/api/put";
  /**
   * YES, that endpoint is v1 for Prometheus v2.0.
   * I don't know why either...
   */
  private static final String QUERY_ENDPOINT = "/api/v1/query";

  private static final String PUSH_GATEWAY_IP_PROPERTY = "ipPushgateway";
  private static final String PUSH_GATEWAY_IP_PROPERTY_DEFAULT = "localhost";
  private static final String PUSH_GATEWAY_PORT_PROPERTY = "portPushgateway";
  private static final int PUSH_GATEWAY_PORT_PROPERTY_DEFAULT = 9091;

  private static final String PROMETHEUS_IP_PROPERTY = "ipPrometheus";
  private static final String PROMETHEUS_IP_PROPERTY_DEFAULT = "localhost";
  private static final String PROMETHEUS_PORT_PROPERTY = "portPrometheus";
  private static final int PROMETHEUS_PORT_PROPERTY_DEFAULT = 9090;

  private static final String USE_COUNT_PROPERTY = "useCount";
  private static final boolean USE_COUNT_PROPERTY_DEFAULT = true;

  private static final String USE_PLAINTEXT_PROPERTY = "plainTextFormat";
  private static final boolean USE_PLAINTEXT_PROPERTY_DEFAULT = true;

  // configurable property holders
  private String ipPushgateway;
  private int portPushgateway;
  private boolean useCount;
  private boolean usePlainTextFormat;

  // internal workings definitions
  private CloseableHttpClient client;
  private URL urlQuery = null;
  private URL urlPut = null;
  private int retries = 3;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    final Properties properties = getProperties();
    if (debug) {
      LOGGER.info("The following properties are given: ");
      for (String element : properties.stringPropertyNames()) {
        LOGGER.info("{}: {}", element, properties.getProperty(element));
      }
    }
    String ipPrometheus = properties.getProperty(PROMETHEUS_IP_PROPERTY, PROMETHEUS_IP_PROPERTY_DEFAULT);
    int portPrometheus = properties.containsKey(PROMETHEUS_PORT_PROPERTY)
        ? Integer.parseInt(properties.getProperty(PROMETHEUS_PORT_PROPERTY))
        : PROMETHEUS_PORT_PROPERTY_DEFAULT;

    ipPushgateway = properties.getProperty(PUSH_GATEWAY_IP_PROPERTY, PUSH_GATEWAY_IP_PROPERTY_DEFAULT);
    portPushgateway = properties.containsKey(PUSH_GATEWAY_PORT_PROPERTY)
        ? Integer.parseInt(properties.getProperty(PUSH_GATEWAY_PORT_PROPERTY))
        : PUSH_GATEWAY_PORT_PROPERTY_DEFAULT;

    usePlainTextFormat = properties.containsKey(USE_PLAINTEXT_PROPERTY)
        ? Boolean.parseBoolean(properties.getProperty(USE_PLAINTEXT_PROPERTY))
        : USE_PLAINTEXT_PROPERTY_DEFAULT;
    useCount = properties.containsKey(USE_COUNT_PROPERTY)
        ? Boolean.parseBoolean(properties.getProperty(USE_COUNT_PROPERTY))
        : USE_COUNT_PROPERTY_DEFAULT;

    RequestConfig requestConfig = RequestConfig.custom().build();
    if (!test) {
      if (!properties.containsKey(PROMETHEUS_IP_PROPERTY)) {
        throwMissingProperty(PROMETHEUS_IP_PROPERTY);
      }
      if (!properties.containsKey(PROMETHEUS_PORT_PROPERTY)) {
        throwMissingProperty(PROMETHEUS_PORT_PROPERTY);
      }
      if (!properties.containsKey(PUSH_GATEWAY_IP_PROPERTY)) {
        throwMissingProperty(PUSH_GATEWAY_IP_PROPERTY);
      }
      if (!properties.containsKey(PUSH_GATEWAY_PORT_PROPERTY)) {
        throwMissingProperty(PUSH_GATEWAY_PORT_PROPERTY);
      }
      try {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }

    try {
      urlQuery = new URL("http", ipPrometheus, portPrometheus, QUERY_ENDPOINT);
      if (debug) {
        LOGGER.info("URL: {}", urlQuery);
      }
      urlPut = new URL("http", ipPushgateway, portPushgateway, PUT_ENDPOINT);
      if (debug) {
        LOGGER.info("URL: {}", urlPut);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
  }

  /**
   * Run the given queryString against the given URL using a HTTP PUT request and return the results as JSONArray
   *
   * @param url         The URL to run the queryString against
   * @param queryString the queryString to send to the URL
   * @return One of the following:
   * <ul>
   * <li>An empty JSONArray, if now results were returned, but the response indicated a success.</li>
   * <li>A JSONArray containing the results, if there were any.</li>
   * <li><tt>null</tt> if the request failed more than {@link #retries} times
   * or an Exception occurred when reading a success response.</li>
   * </ul>
   */
  private JSONArray runInsert(URL url, String queryString) {
    if (url == null) {
      return null;
    }
    HttpResponse response = null;
    try {
      HttpPut postMethod = new HttpPut(url.toURI());
      StringEntity requestEntity = new StringEntity(queryString + "\n");
      requestEntity.setContentType("text/html; charset=UTF-8\\n; version=0.0.4;");
      postMethod.addHeader("host", ipPushgateway);
      postMethod.addHeader("Accept", "application/json");
      postMethod.setEntity(requestEntity);
      try {
        for (int attempt = 0; attempt < retries; attempt++) {
          try {
            response = client.execute(postMethod);
            break;
          } catch (IOException e) {
            // response must be null here, so we can't quietly consume it's entity
          }
        }
        if (response == null) {
          LOGGER.error("Connection to {} failed {} times.", url, retries);
          return null;
        }
      } finally {
        postMethod.releaseConnection();
      }

      if (response.getStatusLine().getStatusCode() >= HttpURLConnection.HTTP_OK
          // between 200 and 300
          && response.getStatusLine().getStatusCode() < HttpURLConnection.HTTP_MULT_CHOICE) {
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_ACCEPTED
            // The pushgateway does not include an entity in the response when inserting
            && response.getEntity().getContent().available() == 0) {
          return new JSONArray();
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
          try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            return new JSONArray(br.lines().collect(Collectors.joining()));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to query {} for '{}' due to {}", url.toString(), queryString, e);
      return null;
    } finally {
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
    return new JSONArray();
  }

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

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.isEmpty() || !METRIC_REGEX.matcher(metric).matches() || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (urlPut == null || urlQuery == null) {
      LOGGER.warn("Binding initialization failed, yet read was called.");
      return Status.UNEXPECTED_STATE;
    }

    StringBuilder queryString = new StringBuilder();
    // Construct query
    queryString.append("{");
    for (Map.Entry<String, List<String>> tagDefinition : tags.entrySet()) {
      queryString.append(tagDefinition.getKey()).append("=~\"");
      for (String tag : tagDefinition.getValue()) {
        queryString.append(tag).append("|");
      }
      queryString.replace(queryString.length() - 1, queryString.length(), "\",");
    }
    queryString.replace(queryString.length() - 1, queryString.length(), "}");

    try {
      queryString = new StringBuilder(URLEncoder.encode(queryString.toString(), StandardCharsets.UTF_8.displayName()));
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("Failed to URL-encode query string as UTF-8");
      return Status.ERROR;
    }

    queryString.insert(0, "?query=" + metric);
    queryString.append("&time=").append(RFC_3339_FORMAT.format(new Date(timestamp)).replace("+", "%2B"));
    if (debug) {
      LOGGER.info("Input Query: {}{}", urlQuery, queryString);
    }
    if (test) {
      return Status.OK;
    }
    HttpResponse response = null;
    HttpGet getMethod = new HttpGet(urlQuery.toString() + queryString);
    try {
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          response = client.execute(getMethod);
          final String content;
          try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            content = br.lines().collect(Collectors.joining());
          }
          JSONObject responseData = new JSONObject(content);
          if (responseData.getString("status").equals("success")) {
            return Status.OK;
          }
        } catch (IOException e) {
          if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
          }
        }
      }
    } finally {
      getMethod.releaseConnection();
    }
    LOGGER.error("Connection to {} failed {} times.", urlQuery, retries);
    return Status.ERROR;
  }


  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.isEmpty() || !METRIC_REGEX.matcher(metric).matches()
        || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    if (urlPut == null || urlQuery == null) {
      LOGGER.warn("Binding initialization failed, yet scan was called.");
      return Status.UNEXPECTED_STATE;
    }

    StringBuilder queryString = new StringBuilder();
    queryString.append("{");
    for (Map.Entry<String, List<String>> tagDefinition : tags.entrySet()) {
      queryString.append(tagDefinition.getKey()).append("=~\"");
      for (String tag : tagDefinition.getValue()) {
        queryString.append(tag).append("|");
      }
      queryString.replace(queryString.length() - 1, queryString.length(), "\",");
    }
    queryString.replace(queryString.length() - 1, queryString.length(), "}");

     /* Application of aggregations by bucket not possible, timeValue and timeUnit ignored
     query_range would not be suitable, as only 11.000 entries are possible
     and those are made up interpolated values and those cannot be aggregated because of the response format */
    long currentTime = new Date().getTime();
    double duration = Math.ceil((endTs - startTs) / 1000d);
    double offset = Math.floor((currentTime - endTs) / 1000d);
    if ((currentTime - offset - duration) > (startTs / 1000d)) {
      duration++;
    }

    // Duration is converted to seconds anyway, so always use those
    // No application of functions on buckets possible, timeValue is ignored
    NumberFormat durationOffsetFormat = new DecimalFormat("###");
    queryString.append("[")
        .append(durationOffsetFormat.format(duration))
        .append("s]offset ")
        .append(durationOffsetFormat.format(offset))
        .append("s)");
    try {
      queryString = new StringBuilder(URLEncoder.encode("(" + metric + queryString,
          StandardCharsets.UTF_8.displayName()));
    } catch (UnsupportedEncodingException e) {
      return Status.ERROR;
    }
    switch (aggreg) {
    case AVERAGE:
      queryString.insert(0, "?query=avg_over_time");
      break;
    case COUNT:
      if (useCount) {
        queryString.insert(0, "?query=count_over_time");
      } else {
        queryString.insert(0, "?query=min_over_time");
      }
      break;
    case SUM:
      queryString.insert(0, "?query=sum_over_time");
      break;
    case NONE:
    default:
      queryString.insert(0, "?query=min_over_time");
      break;
    }
    if (debug) {
      LOGGER.info("Input Query: {}{}", urlQuery, queryString);
    }
    if (test) {
      return Status.OK;
    }
    HttpGet getMethod = new HttpGet(urlQuery.toString() + queryString);
    HttpResponse response = null;
    try {
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          JSONObject responseData;
          response = client.execute(getMethod);
          try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            responseData = new JSONObject(br.lines().collect(Collectors.joining()));
          }
          if (responseData.getString("status").equals("success") && responseData.has("data")) {
            JSONObject dataObject = responseData.getJSONObject("data");
            if (dataObject.has("result") && dataObject.getJSONArray("result").length() > 0) {
              return Status.OK;
            } else {
              return Status.NOT_FOUND;
            }
          }
        } catch (JSONException | IOException e) {
          if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
          }
          response = null;
        }
      }
      LOGGER.error("Connection to {} failed {} times.", urlQuery, retries);
      return Status.ERROR;
    } finally {
      getMethod.releaseConnection();
    }
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.isEmpty() || !METRIC_REGEX.matcher(metric).matches() || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (urlPut == null || urlQuery == null) {
      LOGGER.warn("Binding initialization failed, yet read was called.");
      return Status.UNEXPECTED_STATE;
    }
    if (usePlainTextFormat) {
      StringBuilder queryString = new StringBuilder("#TYPE " + metric + " gauge\n" + metric);
      if (tags.isEmpty()) {
        queryString.append(" ").append(value).append(" ").append(timestamp);
      } else {
        queryString.append("{");
        for (Map.Entry<String, ByteIterator> tagDefinition : tags.entrySet()) {
          queryString.append(tagDefinition.getKey()).append("=\"").append("[");
          queryString.append(tagDefinition.getValue().toString()
              .replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("\n", "\\n"));
          queryString.append("]").append("\",");
        }
        queryString.replace(queryString.length() - 1, queryString.length(), "} ")
            .append(value)
            .append(" ")
            .append(timestamp);
      }

      try {
        if (debug) {
          LOGGER.info("Timestamp: {}", timestamp);
          LOGGER.info("Input Query String: {}", queryString);
        }
        if (test) {
          return Status.OK;
        }
        JSONArray jsonArr = runInsert(urlPut, queryString.toString());
        if (debug) {
          LOGGER.info("jsonArr: {}", jsonArr);
        }
        if (jsonArr == null) {
          LOGGER.error("Failed to process insert to metric {}", metric);
          return Status.ERROR;
        }
        return Status.OK;
      } catch (Exception e) {
        LOGGER.error("Failed to process insert to metric {} due to {}", metric, e);
        return Status.ERROR;
      }
    } else {
      // No usage of custom timestamps possible
      CollectorRegistry registry = new CollectorRegistry();
      Gauge gauge = Gauge.build().name(metric)
          .help(timestamp.toString())
          .labelNames(tags.keySet().toArray(new String[]{}))
          .create();
      Gauge.Child child = gauge.labels(tags.keySet().toArray(new String[]{}));
      child.set(value);
      gauge.register(registry);

      PushGateway gateway = new PushGateway(ipPushgateway + ":" + portPushgateway);
      for (int attempt = 0; attempt < retries; attempt++) {
        try {
          gateway.pushAdd(registry, "test_job");
          return Status.OK;
        } catch (IOException exception) {
          LOGGER.warn("Insert failed with exception {}", exception);
        }
      }
      LOGGER.error("Insert failed {} times", retries);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

