/*
 * Copyright (c) 2015 - 2018 Michael Zimmermann, 2018 YCSB Contributors All rights reserved.
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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Gnocchi client for YCSB framework. Gnocchi doesn't support tags.
 *
 * @author Michael Zimmermann
 */
public class GnocchiClient extends TimeseriesDB {

  private static final String POLICY = "policy";
  private static final String POLICY_URL = "/v1/archive_policy";
  private static final String METRIC_URL = "/v1/metric";
  private static final String METRIC_MEASURES_URL = "/v1/metric/%s/measures";

  private URL measuresURL = null;
  private UUID id;

  private String name = "usermetric";

  private String ip = "localhost";
  private int port = 8041;
  private int retries = 3;
  private CloseableHttpClient client;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  public void init() throws DBException {
    super.init();
    try {
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);

      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      URL metricURL = new URL("http", ip, port, METRIC_URL);
      if (debug) {
        System.out.println("METRIC_URL: " + metricURL);
      }
      id = getMetricID(metricURL);

      // if id==null create the Metric and the ArchivePolicy
      if (id == null) {
        URL policyURL = new URL("http", ip, port, POLICY_URL);
        if (debug) {
          System.out.println("POLICY_URL: " + policyURL);
        }
        if (!createArchivePolicy(policyURL)) {
          throw new DBException("Couldn't create the ArchivePolicy.");
        }
        id = createMetric(metricURL);
        if (id == null && !test) {
          throw new DBException("Couldn't create a Metric.");
        }
      }

      if (debug) {
        System.out.println("ID: " + id);
      }
      measuresURL = new URL("http", ip, port, String.format(METRIC_MEASURES_URL, id));
      if (debug) {
        System.out.println("METRIC_MEASURES_URL: " + measuresURL);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
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

  // {
  // "archive_policy_name": "low",
  // "name": "usermetric"
  // }
  private UUID createMetric(URL metricURL) {

    JSONObject metricObject = new JSONObject();
    metricObject.put("archive_policy_name", POLICY);
    metricObject.put("name", name);

    String queryString = metricObject.toString();

    if (debug) {
      System.out.println("QueryString for Metric creation: " + queryString);
    }

    UUID uuid = doCreateMetric(metricURL, queryString);

    return uuid;

  }

  /*
  {
   "aggregation_methods": [
     "sum",
     "mean",
     "count"
   ],
   "back_window": 0,
   "definition": [
     {
       "granularity": "1s",
       "points": 1000000,
     }
   ],
   "name": "policy"
 }
 */
  private boolean createArchivePolicy(URL policyURL) {

    JSONObject archivePolicyObject = new JSONObject();

    JSONArray aggregationMethodsArray = new JSONArray();
    aggregationMethodsArray.put("sum");
    aggregationMethodsArray.put("mean");
    aggregationMethodsArray.put("count");

    archivePolicyObject.put("aggregation_methods", aggregationMethodsArray);
    archivePolicyObject.put("back_window", 0);

    JSONArray definitionArray = new JSONArray();
    JSONObject definitionObject = new JSONObject();
    definitionObject.put("points", 1000000);
    definitionObject.put("granularity", "1s");
    definitionArray.put(definitionObject);

    archivePolicyObject.put("definition", definitionArray);

    archivePolicyObject.put("name", POLICY);

    String queryString = archivePolicyObject.toString();

    if (debug) {
      System.out.println("QueryString for Archive Policy creation: " + queryString);
    }
    if (test) {
      return true;
    }

    Integer statusCode = doPost(policyURL, queryString);
    if (debug) {
      System.out.println("Returning StatusCode: " + statusCode);
    }
    return statusCode == HttpURLConnection.HTTP_CREATED;
  }

  private Integer doPost(URL url, String queryStr) {
    Integer statusCode;
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      postMethod.addHeader("X-Roles", "admin");

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

      statusCode = response.getStatusLine().getStatusCode();
      EntityUtils.consumeQuietly(response.getEntity());
      postMethod.releaseConnection();
    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return statusCode;
  }

  private UUID doCreateMetric(URL url, String queryStr) {
    UUID uuid = null;
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      postMethod.addHeader("X-Roles", "admin");

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

      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_CREATED
          && response.getEntity().getContentLength() > 0) {

        BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = bis.readLine()) != null) {
          builder.append(line);
        }
        JSONObject jsonObject = new JSONObject(builder.toString());
        uuid = UUID.fromString((String) jsonObject.get("id"));
      }
      EntityUtils.consumeQuietly(response.getEntity());
      postMethod.releaseConnection();
    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return uuid;
  }

  private UUID getMetricID(URL metricURL) {

    UUID uuid = null;

    JSONArray jsonArray = doGet(metricURL);
    for (int n = 0; n < jsonArray.length(); n++) {
      JSONObject jsonObject = jsonArray.getJSONObject(n);
      uuid = UUID.fromString((String) jsonObject.get("id"));
    }
    return uuid;
  }

  private JSONArray doGet(URL url) {
    JSONArray jsonArray = new JSONArray();
    HttpResponse response = null;
    try {
      HttpGet getMethod = new HttpGet(url.toString());
      getMethod.addHeader("accept", "application/json");
      getMethod.addHeader("X-Roles", "admin");

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(getMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            getMethod.releaseConnection();
            return null;
          }
        }
      }

      int statusCode = response.getStatusLine().getStatusCode();
      if (debug) {
        System.out.println("Query StatusCode: " + statusCode);
      }

      if (statusCode == HttpURLConnection.HTTP_OK && response.getEntity().getContentLength() > 0) {
        try (BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
          jsonArray = new JSONArray(bis.lines().collect(Collectors.joining()));
        }
      }
      EntityUtils.consumeQuietly(response.getEntity());
      getMethod.releaseConnection();

    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + ".");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }

    return jsonArray;
  }

  // GET
  // /v1/metric/76f02203-81ce-4dae-bbaa-10de7b9b5701/measures?start=2014-10-06T14:34&stop=2014-10-06T14:34
  // HTTP/1.1
  //
  // [
  // [
  // "2014-10-06T14:34",
  // 1800.0,
  // 19.033333333333335
  // ]
  // ]

  /**
   * @inheritDoc
   */
  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      String timestampString = new Timestamp(timestamp).toString().replace(" ", "T");

      // start and stop can't be the same value; thus increase stop
      // timestamp with 1 ms
      String urlStr = String.format("%s?start=%s&stop=%s", measuresURL.toString(), timestampString,
          timestampString + "1");
      URL newQueryURL = new URL(urlStr);

      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }
      if (test) {
        return Status.OK;
      }

      JSONArray jsonArray = doGet(newQueryURL);
      if (debug) {
        System.out.println("Answer: " + jsonArray.toString());
      }

      if (jsonArray == null || jsonArray.length() < 1) {

        System.err.println("ERROR: Found no values for metric: " + metric + ".");
        return Status.NOT_FOUND;
      } else if (jsonArray.length() > 1) {
        System.err.println("ERROR: Found more than one value for metric: " + metric + " for timestamp: "
            + timestamp + ".");
        return Status.UNEXPECTED_STATE;
      }

    } catch (MalformedURLException e) {
      System.err.println("ERROR: a malformed URL was generated.");
      return Status.ERROR;
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /*
  GET
  /v1/metric/76f02203-81ce-4dae-bbaa-10de7b9b5701/measures?start=2014-10-06T14:34&stop=2014-10-06T14:34&aggregation=max
  HTTP/1.1

  [
    [
      "2014-10-06T14:30:00+00:00",
      1800.0,
      19.033333333333335
    ]
  ]
  */
  /**
   * @inheritDoc
   */
  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    String aggregation = "";
    if (aggreg == AggregationOperation.AVERAGE) {
      aggregation = "&aggregation=mean";
    } else if (aggreg == AggregationOperation.COUNT) {
      aggregation = "&aggregation=count";
    } else if (aggreg == AggregationOperation.SUM) {
      aggregation = "&aggregation=sum";
    }

    try {

      String urlStr = String.format("%s?start=%s&stop=%s%s", measuresURL.toString(),
          startTs.toString().replace(" ", "T"), endTs.toString().replace(" ", "T"), aggregation);
      URL newQueryURL = new URL(urlStr);

      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }
      if (test) {
        return Status.OK;
      }

      JSONArray jsonArray = doGet(newQueryURL);
      if (debug) {
        System.out.println("Answer: " + jsonArray.toString());
      }

      if (jsonArray == null || jsonArray.length() < 1) {
        System.err.println("ERROR: Found no values for metric: " + metric + ".");
        return Status.NOT_FOUND;
      }

    } catch (MalformedURLException e) {
      System.err.println("ERROR: a malformed URL was generated.");
      return Status.ERROR;
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  // POST /v1/metric/76f02203-81ce-4dae-bbaa-10de7b9b5701/measures
  // HTTP/1.1
  // Content-Length: 198
  // Content-Type: application/json
  //
  // [
  // {
  // "timestamp": "2014-10-06T14:33:57",
  // "value": 43.1
  // }
  // ]

  /**
   * @inheritDoc
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      JSONObject insertObject = new JSONObject();
      insertObject.put("timestamp", timestamp.toString().replace(" ", "T"));
      insertObject.put("value", value);

      JSONArray insertArray = new JSONArray();
      insertArray.put(insertObject);

      String query = insertArray.toString();

      if (debug) {
        System.out.println("Insert measures String: " + query);
        System.out.println("Insert measures URL: " + measuresURL.toString());
      }
      if (test) {
        return Status.OK;
      }
      Integer statusCode = doPost(measuresURL, query);

      if (debug) {
        System.out.println("StatusCode: " + statusCode);
      }
      if (statusCode != HttpURLConnection.HTTP_ACCEPTED) {
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

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

}
