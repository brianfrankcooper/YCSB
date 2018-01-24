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

/**
 * Created by Andreas Bader on 20.10.15.
 */

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.*;
import static java.net.HttpURLConnection.*;

/**
 * <p>
 * Blueflood client for YCSB framework. Blueflood has no Tags
 * ({@see https://github.com/rackerlabs/blueflood/wiki/FAQ}).
 * SUM is not available
 * ({@see https://github.com/rackerlabs/blueflood/wiki/10minuteguide#send-numeric-metrics}), using MIN instead.
 * Blueflood only supports the following granularities:
 * <ul>
 * <li>FULL</li>
 * <li>MIN5</li>
 * <li>MIN60</li>
 * <li>MIN240</li>
 * <li>MIN1440</li>
 * </ul>
 * This means that we can't use one bucket over an interval, biggest bucket is Min1440. Full = Ingested Resolution
 * </p>
 */
public class BluefloodClient extends TimeseriesDB {

  private URL urlQuery = null;
  private URL urlIngest = null;
  private String ip = "localhost";
  private String queryURL = "/v2.0/%s/views";
  private String ingestURL = "/v2.0/%s/ingest";
  private String tenantId = "usermetric";
  private int ingestPort = 19000;
  private int queryPort = 19001;
  private CloseableHttpClient client;
  private int ttl = 60 * 60 * 24 * 365; // 1 Year TTL
  private int retries = 3;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      test = Boolean.parseBoolean(getProperties().getProperty("test", "false"));
      ingestPort = Integer.parseInt(getProperties().getProperty("ingestPort", String.valueOf(ingestPort)));
      queryPort = Integer.parseInt(getProperties().getProperty("queryPort", String.valueOf(queryPort)));
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);
      ttl = Integer.parseInt(getProperties().getProperty("ttl", String.valueOf(ttl)));
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
      urlQuery = new URL("http", ip, queryPort, String.format(queryURL, tenantId));
      if (debug) {
        System.out.println("URL: " + urlQuery);
      }
      urlIngest = new URL("http", ip, ingestPort, String.format(ingestURL, tenantId));
      if (debug) {
        System.out.println("URL: " + urlIngest);
      }
    } catch (MalformedURLException e) {
      throw new DBException(e);
    }
  }

  private JSONObject runQuery(URL url, String queryStr) {
    JSONObject jsonObj = new JSONObject();
    HttpResponse response = null;
    try {
      HttpRequestBase method = null;
      if (queryStr != null) {
        HttpPost postMethod = new HttpPost(url.toString());
        StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
        postMethod.setEntity(requestEntity);
        postMethod.addHeader("accept", "application/json");
        method = postMethod;
      } else {
        HttpGet getMethod = new HttpGet(url.toString());
        getMethod.addHeader("accept", "application/json");
        method = getMethod;
      }

      int tries = retries + 1;
      while (true) {
        tries--;
        try {
          response = client.execute(method);
          break;
        } catch (IOException e) {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            e.printStackTrace();
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            method.releaseConnection();
            return null;
          }
        }
      }
      StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() == HTTP_OK ||
          statusLine.getStatusCode() == HTTP_NO_CONTENT ||
          statusLine.getStatusCode() == HTTP_MOVED_PERM) {
        if (statusLine.getStatusCode() == HTTP_MOVED_PERM) {
          System.err.println("WARNING: Query returned 301");
        }
        if (statusLine.getStatusCode() != HTTP_NO_CONTENT && response.getEntity().getContentLength() > 0) {
          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
          BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          StringBuilder builder = new StringBuilder();
          String line;
          while ((line = bis.readLine()) != null) {
            builder.append(line);
          }
          jsonObj = new JSONObject(builder.toString());
        }
        EntityUtils.consumeQuietly(response.getEntity());
        method.releaseConnection();
      }
    } catch (Exception e) {
      System.err.println("ERROR: Errror while trying to query " + url.toString() + " for '" + queryStr + "'.");
      e.printStackTrace();
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return jsonObj;
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
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
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    // Problem: You cant ask for a timestamp at TS=x, you need to give a range.
    // Ergo: Begin: timestamp, End: timestamp + 1 ms
    // Otherwise you get "paramter 'to' must be greater than 'from'"
    // We may get more than that, but we just take the right one
    // There could also be more of them, so count
    try {
      int counter = 0;
      long timestampLong = timestamp;
      String urlStr = String.format("%s/%s?from=%s&to=%s&resolution=FULL", urlQuery.toString(), metric,
          timestampLong, timestampLong + 1);
      URL newQueryURL = new URL(urlStr);
      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }
      JSONObject jsonObj = runQuery(newQueryURL, null);
      if (debug) {
        System.out.println("Answer: " + jsonObj);
      }
      if (jsonObj.has("values")) {
        JSONArray jsonArr = jsonObj.getJSONArray("values");
        if (jsonArr.length() > 1) {
          System.err.println("WARNING: More than 1 value found for READ.");
        }
        if (jsonArr.length() > 0) {
          for (int i = 0; i < jsonArr.length(); i++) {
            JSONObject jsonObject = jsonArr.getJSONObject(i);
            if (jsonObject.has("timestamp") && jsonObject.getLong("timestamp") == timestampLong) {
              counter++;
            }
          }
        }
      }
      if (counter == 0) {
        System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
        return Status.NOT_FOUND;
      } else if (counter > 1) {
        System.err.println("ERROR: Found multiple values for metric: " + metric + " for timestamp: " + timestamp + ".");
      }
    } catch (MalformedURLException e) {
      System.err.println("ERROR: a malformed URL was generated.");
      return Status.ERROR;
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String,
      List<String>> tags, AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric.equals("")) {
      return Status.BAD_REQUEST;
    }
    if (startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    String urlAppendix = "";
    long granularity = TimeUnit.MILLISECONDS.convert(timeValue, timeUnit);
    if (aggreg != NONE) {
      if (granularity == TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)) {
        urlAppendix = "&resolution=MIN5";
      } else if (granularity == TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES)) {
        urlAppendix = "&resolution=MIN60";
      } else if (granularity == TimeUnit.MILLISECONDS.convert(240, TimeUnit.MINUTES)) {
        urlAppendix = "&resolution=MIN240";
      } else if ((granularity == TimeUnit.MILLISECONDS.convert(1440, TimeUnit.MINUTES) || timeValue == 0)) {
        urlAppendix = "&resolution=MIN1440";
      } else {
        // Only when timeValue != 0, but needs no if here
        System.err.print("WARNING: Blueflood only supports 5, 60, 240, 1440 as granularity, using full granularity," +
            " which is the granularity that was used to ingest data.");
        urlAppendix = "&resolution=FULL";
      }
    } else {
      urlAppendix = "&resolution=FULL";
    }
    if (aggreg == AVERAGE) {
      urlAppendix += "&select=average";
    } else if (aggreg == COUNT) {
      urlAppendix += "&select=numPoints";
    } else if (aggreg == SUM) {
      urlAppendix += "&select=min";
    }
    try {
      String urlStr = String.format("%s/%s?from=%s&to=%s%s", urlQuery.toString(), metric, startTs, endTs, urlAppendix);
      URL newQueryURL = new URL(urlStr);
      if (debug) {
        System.out.println("QueryURL: " + newQueryURL.toString());
      }
      JSONObject jsonObj = runQuery(newQueryURL, null);
      if (debug) {
        System.out.println("Answer: " + jsonObj);
      }
      if (jsonObj.has("values")) {
        JSONArray jsonArr = jsonObj.getJSONArray("values");
        if (jsonArr.length() < 1) {
          return Status.NOT_FOUND;
        }
      } else {
        return Status.NOT_FOUND;
      }
      if (!jsonObj.has("metadata")) {
        return Status.NOT_FOUND;
      }
      // Could check further here, but costs to much time, we expect the db to work right
      // And since we don't filter for tags (not supported), there is no possibility that we have an empty answer
    } catch (MalformedURLException e) {
      System.err.println("ERROR: a malformed URL was generated.");
      return Status.ERROR;
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * @inheritDoc
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    try {
      JSONObject query = new JSONObject();
      query.put("collectionTime", timestamp);
      query.put("ttlInSeconds", this.ttl);
      query.put("metricValue", value);
      query.put("metricName", metric);
      JSONArray jsonArr = new JSONArray();
      jsonArr.put(query);
      if (debug) {
        System.out.println("Input Query String: " + query.toString());
        System.out.println("Querying URL: " + urlIngest.toString());
      }
      if (test) {
        return Status.OK;
      }
      JSONObject jsonObj = runQuery(urlIngest, jsonArr.toString());
      if (debug) {
        System.out.println("Answer: " + jsonObj);
      }
      if (jsonObj == null) {
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

