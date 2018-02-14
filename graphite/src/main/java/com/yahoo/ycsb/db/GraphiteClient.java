/*
 * Copyright (c) 2015 - 2018 Rene Trefft, 2018 YCSB Contributors All rights reserved.
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
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Graphite client for YCSB framework. Inserts records with the plaintext
 * protocol supported by Carbon. Retrieving is done using the Render API of the
 * Graphite-API.<br>
 * Restrictions:<br>
 * Tags are not supported in Graphite, so they will be ignored. Timestamps are
 * stored in second precision. Records with timestamps in the future can't be
 * stored. The function count is not supported - max will be used instead.
 *
 * @author Rene Trefft
 * @see <a href="https://graphite.readthedocs.org">Graphite Website</a>
 * @see <a href=
 * "https://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol">
 * Graphite - The plaintext protocol</a>
 * @see <a href="https://graphite-api.readthedocs.org">Graphite-API Website</a>
 * @see <a href=
 * "https://graphite-api.readthedocs.org/en/latest/api.html#the-render-api-render">
 * Graphite-API - The Render API</a>
 */
public class GraphiteClient extends TimeseriesDB {

  private static final String IP_PROPERTY = "ip";
  private static final String PLAINTEXT_PORT_PROPERTY = "plaintextPort";
  private static final String GRAPHITE_API_PORT_PROPERTY = "graphiteApiPort";

  /**
   * Inserting with Plaintext receiver of Carbon.
   */
  private Socket plainTextSocket;
  private PrintWriter plainTextWriter;

  /**
   * Retrieving with Render API of Graphite-API.
   */
  private HttpClient renderApiClient;

  /**
   * URL to Render API of Graphite-API.
   */
  private String renderApiUrl;

  private static final String RENDER_API_PATH = "/render?";

  @Override
  public void init() throws DBException {
    super.init();

    Properties properties = getProperties();
    if (!properties.containsKey(IP_PROPERTY) && !test) {
      throwMissingProperty(IP_PROPERTY);
    }
    if (!properties.containsKey(PLAINTEXT_PORT_PROPERTY) && !test) {
      throwMissingProperty(PLAINTEXT_PORT_PROPERTY);
    }
    if (!properties.containsKey(GRAPHITE_API_PORT_PROPERTY) && !test) {
      throwMissingProperty(GRAPHITE_API_PORT_PROPERTY);
    }

    String ip = properties.getProperty("ip");
    int plaintextPort = Integer.parseInt(properties.getProperty(PLAINTEXT_PORT_PROPERTY)); // default 2003?
    int graphiteApiPort = Integer.parseInt(properties.getProperty(GRAPHITE_API_PORT_PROPERTY)); // default 80?
    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : properties.stringPropertyNames()) {
        System.out.println(element + ": " + properties.getProperty(element));
      }
    }
    if (!test) {
      try {
        plainTextSocket = new Socket(ip, plaintextPort);
        plainTextWriter = new PrintWriter(plainTextSocket.getOutputStream(), true);

        renderApiUrl = "http://" + ip + ':' + graphiteApiPort + RENDER_API_PATH;
        renderApiClient = HttpClients.createDefault();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (!test) {
        plainTextWriter.close();
        plainTextSocket.close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.isEmpty() || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    // timestamp is assumed to be millis, we want the second here
    long timestampSec = timestamp / 1000;

    StringBuilder readURL = new StringBuilder(renderApiUrl);
    readURL.append("target=");
    readURL.append(metric);

    // from is exclusive, until is inclusive for time range definition
    readURL.append("&from=");
    readURL.append(timestampSec - 1);
    readURL.append("&until=");

    readURL.append(timestampSec);
    readURL.append("&format=raw");

    HttpGet request = new HttpGet(readURL.toString());

    if (test) {
      return Status.OK;
    }

    try {
      HttpResponse response = renderApiClient.execute(request);
      String responseStr = EntityUtils.toString(response.getEntity());
      if (debug) {
        System.out.println("Read Request: " + readURL + '\n' + "Read Response: " + responseStr);
      }

      String[] responseData = responseStr.split("\\|");
      if (responseData.length <= 1 || responseData[1].trim().equalsIgnoreCase("none")) {
        System.err.printf("ERROR: No value found for metric %s and timestamp %d.%n", metric, timestampSec);
        return Status.NOT_FOUND;
      } else {
        if (debug) {
          System.out.printf("Found value %s for metric %s and timestamp %d.%n",
              responseData[1].trim(), metric, timestampSec);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                  AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.isEmpty() || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }

    StringBuilder scanURL = new StringBuilder(renderApiUrl);
    scanURL.append("target=summarize(");
    scanURL.append(metric);
    scanURL.append(",%22");

    switch (timeUnit) {
    case DAYS:
      scanURL.append(timeValue);
      scanURL.append("day");
      break;
    case HOURS:
      scanURL.append(timeValue);
      scanURL.append("hour");
      break;
    case MINUTES:
      scanURL.append(timeValue);
      scanURL.append("minute");
      break;
    case SECONDS:
      scanURL.append(timeValue);
      scanURL.append("second");
      break;
    default:
      // time unit not supported => convert to whole seconds, precision can be lost
      scanURL.append(TimeUnit.SECONDS.convert(timeValue, timeUnit));
      scanURL.append("second");
      break;
    }

    scanURL.append("%22,%22");

    if (aggreg == AggregationOperation.AVERAGE) {
      scanURL.append("avg");
    } else if (aggreg == AggregationOperation.SUM) {
      scanURL.append("sum");
    } else {
      // if no function or unsupported count is passed
      scanURL.append("max");
    }

    long startTimestampSec = startTs / 1000;
    long endTimestampSec = endTs / 1000;

    // last param of summarize function is set to true to create buckets
    // starting at the from time
    scanURL.append("%22,true)&from=");
    scanURL.append(startTimestampSec - 1);
    scanURL.append("&until=");
    scanURL.append(endTimestampSec);
    scanURL.append("&format=raw");

    HttpGet request = new HttpGet(scanURL.toString());

    if (test) {
      return Status.OK;
    }

    try {
      HttpResponse response = renderApiClient.execute(request);
      String responseStr = EntityUtils.toString(response.getEntity());
      if (debug) {
        System.out.printf("Scan Request: %s\nScan Response: %s%n", scanURL, responseStr);
      }

      String[] responseData = responseStr.split("\\|");
      boolean valuesFound = false;
      if (responseData.length > 1) {
        String[] values = responseData[1].trim().split(",");
        for (String value : values) {
          if (!value.equalsIgnoreCase("none")) {
            valuesFound = true;
            break;
          }
        }
      }

      if (!valuesFound) {
        // Workloads call scan with 1 millisecond intervals which will
        // be converted to 0 seconds (milliseconds are not supported).
        // => No values will be correctly returned
        // => Avoid error message
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {

    if (metric == null || metric.isEmpty() || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (test) {
      return Status.OK;
    }

    long timestampSec = timestamp / 1000;
    String insertRequest = String.format("%s %s %d", metric, value, timestampSec);
    if (debug) {
      System.out.println("Insert Request: " + insertRequest);
    }

    // No response, so we doesn't know if new data was accepted /
    // successfully stored => SUCCESS will be always returned
    plainTextWriter.println(insertRequest);
    if (debug) {
      System.out.printf("Inserted metric %s, timestamp %d and value %s.%n", metric, timestampSec, value);
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
