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
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Seriesly client for YCSB framework. It's possible to store tags, as seriesly
 * is schemaless. But the filtering in seriesly doesn't work as the framework
 * expects it.
 *
 * @author Michael Zimmermann
 */
public class SerieslyClient extends TimeseriesDB {

  private String databaseName = "TestDB";
  private String metricFieldName = "metric";
  private String valueFieldName = "value";
  private String tagsFieldName = "tags";

  private String ip = "localhost";
  private int port = 3133;
  private int retries = 3;
  private CloseableHttpClient client;

  private URL databaseURL = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  public void init() throws DBException {
    super.init();
    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
      }
    }
    try {
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));

      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);

      RequestConfig requestConfig = RequestConfig.custom().build();
      if (!test) {
        client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      }

    } catch (Exception e) {
      throw new DBException(e);
    }

    try {
      databaseURL = new URL("http", ip, port, "/" + databaseName);
      if (debug) {
        System.out.println("URL: " + databaseURL);
      }

    } catch (MalformedURLException e) {
      throw new DBException(e);
    }

    if (doPut(databaseURL) != HttpURLConnection.HTTP_CREATED) {
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
    Integer statusCode;
    HttpResponse response = null;
    try {
      HttpPut putMethod = new HttpPut(url.toString());

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(putMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            putMethod.releaseConnection();
            return null;
          }
        }
      }

      statusCode = response.getStatusLine().getStatusCode();
      EntityUtils.consumeQuietly(response.getEntity());
      putMethod.releaseConnection();

    } catch (Exception e) {
      System.err.println("ERROR: Error while trying to send PUT request '" + url.toString() + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }

    return statusCode;
  }

  private Integer doPost(URL url, String queryStr) {
    Integer statusCode;
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(postMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
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

  private JSONObject doGet(URL url) {
    JSONObject jsonObject = new JSONObject();
    HttpResponse response = null;
    try {
      HttpGet getMethod = new HttpGet(url.toString());
      getMethod.addHeader("accept", "application/json");

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(getMethod);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + " times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            getMethod.releaseConnection();
            return null;
          }
        }
      }

      if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {

        BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = bis.readLine()) != null) {
          builder.append(line);
        }

        if (builder.length() > 0) {
          jsonObject = new JSONObject(builder.toString());
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

    return jsonObject;
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      long timestampLong = timestamp;
      String urlStr = String.format("%s/_query?from=%s&to=%s&group=1&ptr=/%s&reducer=any&f=/%s&fv=%s", databaseURL,
          timestampLong, timestampLong, valueFieldName, metricFieldName, metric);
      URL newQueryURL = new URL(urlStr);

      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONObject jsonObject = doGet(newQueryURL);
      if (debug) {
        System.out.println("Answer: " + jsonObject.toString());
      }
      JSONArray jsonArray = jsonObject.getJSONArray(Long.toString(timestampLong));

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
      System.err.println(
          "WARNING: Timeunit " + timeUnit.toString() + " is not supported. Using milliseconds instead!");
      grouping = 1;
    }

    grouping = grouping * timeValue;

    try {
      String urlStr = String.format("%s/_query?from=%s&to=%s&group=%s&ptr=/%s&reducer=%s&f=/%s&fv=%s", databaseURL,
          startTs, endTs, grouping, valueFieldName, aggregation, metricFieldName, metric);
      URL newQueryURL = new URL(urlStr);

      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }

      if (test) {
        return Status.OK;
      }
      JSONObject jsonObject = doGet(newQueryURL);

      if (debug) {
        System.out.println("Answer: " + jsonObject.toString());
      }
      String[] elementNames = JSONObject.getNames(jsonObject);
      JSONArray jsonArray = jsonObject.getJSONArray(elementNames[0]);
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

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      JSONObject insertObject = new JSONObject();
      insertObject.put(metricFieldName, metric);
      insertObject.put(valueFieldName, value);

      if (tags != null && !tags.isEmpty()) {
        JSONObject tagsObject = new JSONObject();
        for (Entry<String, ByteIterator> entry : tags.entrySet()) {
          tagsObject.put(entry.getKey(), entry.getValue().toString());
        }
        insertObject.put(tagsFieldName, tagsObject);
      }

      String query = insertObject.toString();
      String urlStr = String.format("%s?ts=%s", databaseURL.toString(), timestamp);
      URL insertURL = new URL(urlStr);

      if (debug) {
        System.out.println("Insert measures String: " + query);
        System.out.println("Insert measures URL: " + insertURL.toString());
      }
      if (test) {
        return Status.OK;
      }
      Integer statusCode = doPost(insertURL, query);
      if (debug) {
        System.out.println("StatusCode: " + statusCode);
      }
      if (statusCode != HttpURLConnection.HTTP_CREATED) {
        System.err.println("ERROR: Error in processing insert to metric: " + metric);
        return Status.ERROR;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
