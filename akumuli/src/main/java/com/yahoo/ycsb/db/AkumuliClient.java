/*
 * Copyright (c) 2015 - 2017 Andreas Bader, 2018 YCSB Contributors All rights reserved.
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Akumuli client for YCSB-TS framework.<br>
 * Restrictions:<br>
 * Timestamps are stored in nanosecond precision. Functions count and sum are
 * not supported - for those max will be used instead.
 *
 * @author Rene Trefft
 * @author Andreas Bader
 */
public class AkumuliClient extends TimeseriesDB {

  private static final int MILIS_TO_NANOS = 1_000_000;

  public static final String IP_PROPERTY = "ip";
  public static final String TCP_PORT_PROPERTY = "tcpPort";
  public static final String HTTP_PORT_PROPERTY = "httpPort";
  public static final String IP_PROPERTY_DEFAULT = "localhost";
  public static final String TCP_PORT_PROPERTY_DEFAULT = "8282";
  public static final String HTTP_PORT_PROPERTY_DEFAULT = "8181";

  /**
   * Inserting with TCP Server.
   */
  private Socket insertSocket;
  private PrintWriter insertWriter;

  private HttpClient akumuliHTTPClient;

  /**
   * URL to HTTP API.
   */
  private String akumuliHTTPUrl;

  private TimeZone tz;
  private DateFormat df;

  @Override
  public void init() throws DBException {

    if (!getProperties().containsKey(IP_PROPERTY) && !test) {
      throw new DBException("No ip given, abort.");
    }
    if (!getProperties().containsKey(TCP_PORT_PROPERTY) && !test) {
      throw new DBException("No TCP Server port given, abort.");
    }
    if (!getProperties().containsKey(HTTP_PORT_PROPERTY) && !test) {
      throw new DBException("No HTTP Server port given, abort.");
    }

    String ip = getProperties().getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);
    int tcpPort = Integer.parseInt(getProperties().getProperty(TCP_PORT_PROPERTY, TCP_PORT_PROPERTY_DEFAULT));
    int httpPort = Integer.parseInt(getProperties().getProperty(HTTP_PORT_PROPERTY, HTTP_PORT_PROPERTY_DEFAULT));
    tz = TimeZone.getTimeZone("UTC");
    df = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSSSSSS");
    df.setTimeZone(tz);

    if (debug) {
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
      }
    }

    if (test) {
      return;
    }

    try {
      insertSocket = new Socket(ip, tcpPort);
      insertWriter = new PrintWriter(insertSocket.getOutputStream(), false);

      akumuliHTTPUrl = "http://" + ip + ':' + httpPort;
      akumuliHTTPClient = HttpClients.createDefault();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      insertWriter.close();
      insertSocket.close();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.isEmpty() || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    String timestampNanos = df.format(new Timestamp(timestamp));
    JSONObject range = new JSONObject().put("from", timestampNanos).put("to", timestampNanos);

    JSONObject where = new JSONObject();
    for (Map.Entry<String, List<String>> tag : tags.entrySet()) {
      where.put(tag.getKey(), new JSONArray(tag.getValue()));
    }

    JSONObject output = new JSONObject().put("format", "csv");

    JSONObject readQuery;
    if (where.length() > 0) {
      readQuery = new JSONObject().put("metric", metric).put("range", range).put("where", where).put("output", output);
    } else {
      readQuery = new JSONObject().put("metric", metric).put("range", range).put("output", output);
    }



    if (debug) {
      System.out.println("Read Request: " + readQuery.toString());
    }

    if (test) {
      return Status.OK;
    }

    try {
      HttpPost readRequest = new HttpPost(akumuliHTTPUrl);
      readRequest.setEntity(new StringEntity(readQuery.toString()));
      HttpResponse response = akumuliHTTPClient.execute(readRequest);
      String responseStr = EntityUtils.toString(response.getEntity());

      if (debug) {
        System.out.println('\n' + "Read Response: " + responseStr);
      }

      String[] responseData = responseStr.replace(" ", "").split(",");
      if (responseData.length < 3) {
        System.err.println("ERROR: No value found for metric " + metric + ", timestamp " + timestamp.toString()
            + " and tags " + tags.toString() + ".");
        return Status.NOT_FOUND;
      } else if (responseData.length > 3) {
        System.out.println("Found more than one value for metric " + metric + ", timestamp "
            + timestamp.toString() + " and tags " + tags.toString() + ".");
        return Status.UNEXPECTED_STATE;
      } else if (!timestampNanos.equals(responseData[responseData.length - 2])) {
        System.out.println("Found value with other timestamp than expected for metric " + metric + "," +
            " timestamp expected " + timestamp.toString() + " timestamp received " +
            responseData[responseData.length - 2] + " and tags " + tags.toString() + ".");
        return Status.NOT_FOUND;
      } else {
        if (debug) {
          System.out.println("Found value " + responseData[2].trim() + " for metric " + metric
              + ", timestamp " + timestamp.toString() + " and tags " + tags.toString() + ".");
        }
      }
    } catch (Exception exc) {
      exc.printStackTrace();
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

    JSONObject range = new JSONObject().put("from", df.format(new Timestamp(startTs))).put("to",
        df.format(new Timestamp(endTs)));

    JSONObject where = new JSONObject();
    for (Map.Entry<String, List<String>> tag : tags.entrySet()) {
      where.put(tag.getKey(), new JSONArray(tag.getValue()));
    }

    JSONObject sample = new JSONObject();

    // count and sum not supported, for those we will be use max instead
    if (aggreg == AggregationOperation.AVERAGE) {
      sample.put("name", "paa");
    } else {
      sample.put("name", "max-paa");
    }

    JSONObject groupBy = new JSONObject();
    if (timeValue != 0) {
      switch (timeUnit) {
      case HOURS:
        groupBy.put("time", timeValue + "h");
        break;
      case MINUTES:
        groupBy.put("time", timeValue + "m");
        break;
      case SECONDS:
        groupBy.put("time", timeValue + "s");
        break;
      case MILLISECONDS:
        groupBy.put("time", timeValue + "ms");
        break;
      case MICROSECONDS:
        groupBy.put("time", timeValue + "us");
        break;
      case NANOSECONDS:
        groupBy.put("time", timeValue + "n");
        break;
      default:
        // time unit not supported => convert to whole nanoseconds,
        // precision can be lost
        groupBy.put("time", TimeUnit.NANOSECONDS.convert(timeValue, timeUnit) + "n");
        break;
      }
    } else {
      // if timeValue is zero, one large bucket (bin in Akumuli wiki) is needed
      long timeValueInMs = endTs - startTs;
      timeValueInMs += (timeValueInMs / 100) * 10; // add 10% for safety
      if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)) {
        groupBy.put("time", TimeUnit.DAYS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "d");
      } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS)) {
        groupBy.put("time", TimeUnit.HOURS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "h");
      } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)) {
        groupBy.put("time", TimeUnit.MINUTES.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "m");
      } else if (timeValueInMs > TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS)) {
        groupBy.put("time", TimeUnit.SECONDS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "s");
      } else {
        groupBy.put("time", TimeUnit.MILLISECONDS.convert(timeValueInMs, TimeUnit.MILLISECONDS) + "ms");
      }
    }
    JSONObject output = new JSONObject().put("format", "csv");

    JSONObject scanQuery = new JSONObject().put("metric", metric).put("range", range).put("where", where)
        .append("sample", sample).put("group-by", groupBy).put("output", output);

    if (test) {
      return Status.OK;
    }

    try {
      HttpPost readRequest = new HttpPost(akumuliHTTPUrl);
      readRequest.setEntity(new StringEntity(scanQuery.toString()));
      HttpResponse response = akumuliHTTPClient.execute(readRequest);
      String responseStr = EntityUtils.toString(response.getEntity());
      if (debug) {
        System.out.println("Scan Request: " + scanQuery.toString() + '\n' + "Scan Response: " + responseStr);
      }

      String[] responseData = responseStr.split(",");
      if (responseData.length < 3) {
        // allowed to happen, no error message
        return Status.NOT_FOUND;
      } else {
        if (debug) {
          System.out.println("Found value(s) for metric " + metric + ", start timestamp " + startTs.toString()
              + ", end timestamp " + endTs.toString() + ", aggregation=" + aggreg
              + ", time value " + timeValue + ", time unit " + timeUnit + " and tags "
              + tags.toString() + ".");
        }
      }
    } catch (Exception exc) {
      exc.printStackTrace();
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

    StringBuilder insertRequest = new StringBuilder("+");
    insertRequest.append(metric);

    for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
      insertRequest.append(' ');
      insertRequest.append(tag.getKey());
      insertRequest.append('=');
      insertRequest.append(tag.getValue().toString());
    }

    insertRequest.append("\r\n:");
    // Conversion ms -> ns
    insertRequest.append(timestamp * MILIS_TO_NANOS);
    insertRequest.append("\r\n+");
    insertRequest.append(value);
    insertRequest.append("\r\n");

    if (debug) {
      System.out.println("Insert Request:\n" + insertRequest);
    }

    // No response, so we doesn't know if new data was accepted /
    // successfully stored => SUCCESS will be always returned
    insertWriter.print(insertRequest);
    insertWriter.flush();

    if (debug) {
      System.out.println("Inserted metric " + metric + ", timestamp " + timestamp + ", value " + value
          + " and tags " + tags + ".");
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    // Akumuli currently does not support delete
    return Status.NOT_IMPLEMENTED;
  }
}
