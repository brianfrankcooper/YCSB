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

import com.sun.net.ssl.internal.www.protocol.https.HttpsURLConnectionOldImpl;
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
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Prometheus client for YCSB framework.
 */
public class PrometheusClient extends TimeseriesDB {

  private URL urlQuery = null;
  private URL urlPut = null;
  private String ipPushgateway = "localhost";
  private String ipPrometheus = "localhost";
  private String putURL = "/api/put";
  private int portPushgateway = 9091;
  private int portPrometheus = 9090;
  private boolean useCount = true;
  private boolean usePlainTextFormat = true;
  private CloseableHttpClient client;
  private int retries = 3;

  private static final DateFormat RFC_3339_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  private static final String METRIC_REGEX = "[a-zA-Z_:][a-zA-Z0-9_:]*";
  private String queryURLInfix = "/api/v1/query";

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      if (!getProperties().containsKey("ipPrometheus") && !test) {
        throw new DBException("No ip_server given, abort.");
      }
      ipPrometheus = getProperties().getProperty("ipPrometheus", ipPrometheus);

      if (!getProperties().containsKey("portPrometheus") && !test) {
        throw new DBException("No portPrometheus given, abort.");
      }
      portPrometheus = Integer.parseInt(getProperties().getProperty("portPrometheus", String.valueOf(portPrometheus)));

      if (!getProperties().containsKey("ipPushgateway") && !test) {
        throw new DBException("No ipPushgateway given, abort.");
      }
      ipPushgateway = getProperties().getProperty("ipPushgateway", ipPushgateway);
      if (!getProperties().containsKey("portPushgateway") && !test) {
        throw new DBException("No portPushgateway given, abort.");
      }
      portPushgateway = Integer.parseInt(getProperties()
          .getProperty("portPushgateway", String.valueOf(portPushgateway)));

      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
      usePlainTextFormat = Boolean.parseBoolean(getProperties().getProperty("plainTextFormat", "true"));
      useCount = Boolean.parseBoolean(getProperties().getProperty("useCount", "true"));
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      urlQuery = new URL("http", ipPrometheus, portPrometheus, queryURLInfix);
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
      urlPut = new URL("http", ipPushgateway, portPushgateway, putURL);
      if (debug) {
        System.out.println("URL: " + urlPut);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
  }

  private JSONArray runQuery(URL url, String queryStr) {
    JSONArray jsonArr = new JSONArray();
    HttpResponse response = null;
    try {
      HttpPut postMethod = new HttpPut(url.toURI());

      StringEntity requestEntity = new StringEntity(
          queryStr + "\n");
      requestEntity.setContentType("text/html; charset=UTF-8\\n; version=0.0.4;");
      postMethod.addHeader("host", ipPushgateway);
      postMethod.addHeader("Accept", "application/json");
      postMethod.setEntity(requestEntity);

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(postMethod);
          String inputLine = "";
          BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          try {
            while ((inputLine = br.readLine()) != null) {
              System.out.println("1" + inputLine);
            }
            br.close();
          } catch (IOException e) {
            //e.printStackTrace();
          }
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
      if (response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300) {
        if (response.getStatusLine().getStatusCode() == HttpsURLConnectionOldImpl.HTTP_ACCEPTED) {
          // The pushgateway does not include an entity in the response when inserting
          if (response.getEntity().getContent().available() == 0) {
            jsonArr = new JSONArray();
            return jsonArr;
          }
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
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
      System.err.println("ERROR: Error while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return jsonArr;
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
    if (metric == null || metric.isEmpty() || !metric.matches(METRIC_REGEX) || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    int tries = retries + 1;
    HttpGet getMethod;
    String queryString = "";
    HttpResponse response = null;
    JSONObject responseData;

    // Construct query
    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      queryString += entry.getKey() + "=~\"";
      List<String> values =  entry.getValue();
      for (int i = 0; i < values.size(); i++) {
        queryString += values.get(i)
            + (i + 1 < (values.size()) ? "|" : "");
      }
      queryString += "\",";
    }
    queryString = "{" + (queryString.isEmpty() ? "" : queryString.substring(0, queryString.length() - 1)) + "}";

    try {
      queryString = URLEncoder.encode(queryString, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return Status.ERROR;
    }

    queryString = "?query=" + metric + queryString;
    queryString += "&time=" + RFC_3339_FORMAT.format(new Date(timestamp)).replace("+", "%2B");

    if (debug) {
      System.out.println("Input Query: " + urlQuery.toString() + queryString);
    }
    getMethod = new HttpGet(urlQuery.toString() + queryString);
    loop:
    while (true) {
      tries--;
      try {
        if (test) {
          return Status.OK;
        }

        response = client.execute(getMethod);

        String inputLine = "";
        String content = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        try {
          while ((inputLine = br.readLine()) != null) {
            content += inputLine;
          }
          br.close();
        } catch (IOException e) {
        }
        responseData = new JSONObject(content);

        if (responseData.getString("status").equals("success")) {
          return Status.OK;
        }
      } catch (IOException e) {
        if (tries < 1) {
          System.err.print("ERROR: Connection to " + urlQuery.toString() + " failed " + retries + "times.");
          e.printStackTrace();
          if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
          }
          EntityUtils.consumeQuietly(response.getEntity());
          getMethod.releaseConnection();
          return Status.ERROR;
        }
        continue loop;
      }
    }
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.isEmpty() || !metric.matches(METRIC_REGEX) || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }

    NumberFormat durationOffsetFormat = new DecimalFormat("###");
    int tries = retries + 1;
    HttpGet getMethod;
    String queryString = "";
    HttpResponse response = null;
    JSONObject responseData;
    double duration;
    double offset;
    double currentTime = new Date().getTime();

    for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
      queryString += entry.getKey() + "=~\"";
      List<String> values = entry.getValue();
      for (int i = 0; i < values.size(); i++) {
        queryString += values.get(i)
            + (i + 1 < (values.size()) ? "|" : "");
      }
      queryString += "\",";
    }

         /* Application of aggregations by bucket not possible, timeValue and timeUnit ignored
         query_range would not be suitable, as only 11.000 entries are possible
         and those are made up interpolated values and those cannot be aggregated because of the response format */
    duration = Math.ceil(((double) endTs - startTs) / 1000d);
    offset = (long) Math.floor((currentTime - endTs) / 1000d);
    if ((currentTime - offset - duration) > (startTs / 1000d)) {
      duration++;
    }

    queryString = "{" + queryString.substring(0, queryString.length() - 1) + "}[" +
        durationOffsetFormat.format(duration) + "s]offset " + durationOffsetFormat.format(offset) + "s)";
    try {
      queryString = URLEncoder.encode("(" + metric + queryString, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return Status.ERROR;
    }
    // Duration are converted to seconds anyway, so always use those
    // No application of functions on buckets possible, timeValue is ignored

    if (aggreg == AggregationOperation.AVERAGE) {
      queryString = "?query=avg_over_time" + queryString;

    } else if (aggreg == AggregationOperation.COUNT) {
      if (useCount) {
        queryString = "?query=count_over_time" + queryString;
      } else {
        queryString = "?query=min_over_time" + queryString;
      }
    } else if (aggreg == AggregationOperation.SUM) {
      queryString = "?query=sum_over_time" + queryString;

    } else {
      queryString = "?query=min_over_time" + queryString;


    }
    if (debug) {
      System.out.println("Input Query: " + urlQuery.toString() + queryString);
    }
    getMethod = new HttpGet(urlQuery.toString() + queryString);
    loop:
    while (true) {
      tries--;
      try {
        if (test) {
          return Status.OK;
        }

        response = client.execute(getMethod);

        String inputLine = "";
        String content = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        try {
          while ((inputLine = br.readLine()) != null) {
            content += inputLine;
          }
          br.close();
        } catch (IOException e) {
        }
        responseData = new JSONObject(content);

        if (responseData.getString("status").equals("success")) {

          try {
            if (responseData.getJSONObject("data").getJSONArray("result").length() > 0) {
              return Status.OK;
            } else {
              return Status.NOT_FOUND;
            }
          } catch (JSONException e) {
            // No data included in response
            EntityUtils.consumeQuietly(response.getEntity());
            getMethod.releaseConnection();
            return Status.ERROR;
          }
        }
      } catch (IOException e) {
        if (tries < 1) {
          System.err.print("ERROR: Connection to " + urlQuery.toString() + " failed " + retries + "times.");
          e.printStackTrace();
          if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
          }
          EntityUtils.consumeQuietly(response.getEntity());
          getMethod.releaseConnection();
          return Status.ERROR;
        }
        continue loop;
      }
    }
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.isEmpty() || !metric.matches(METRIC_REGEX) || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (usePlainTextFormat) {
      String queryString = "#TYPE " + metric + " gauge\n" + metric;
      if (tags.size() > 0) {
        queryString += "{";
        for (String tagKey : tags.keySet()) {
          queryString += tagKey + "=\"" +
              (tags.get(tagKey).toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")) +
              "\",";
        }
        queryString = queryString.substring(0, queryString.length() - 1) + "} " + value + " " + timestamp;
      } else {
        queryString += " " + value + " " + timestamp;
      }

      try {

        if (debug) {
          System.out.println("Timestamp: " + timestamp);
          System.out.println("Input Query String: " + queryString);
        }
        if (test) {
          return Status.OK;
        }
        JSONArray jsonArr = runQuery(urlPut, queryString);
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
      int tries = retries;
      loop:
      while (true) {
        try {
          tries--;
          gateway.pushAdd(registry, "test_job");
          return Status.OK;
        } catch (IOException exception) {
          if (tries > 0) {
            continue loop;
          }
          System.err.println(exception.toString());
          return Status.ERROR;
        }
      }
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

