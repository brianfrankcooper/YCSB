/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
//java site.ycsb.CommandLine -db site.ycsb.webservice.transactionrest.TransactionRestClient
package site.ycsb.transactionwebservice.transactionrest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import org.json.JSONObject;
import site.ycsb.*;

/**
 * Class responsible for making web service requests for benchmarking purpose.
 * Using Apache HttpClient over standard Java HTTP API as this is more flexible
 * and provides better functionality. For example HttpClient can automatically
 * handle redirects and proxy authentication which the standard Java API can't.
 */
public class TransactionRestClient extends DB {

  private static final String URL_PREFIX = "url.prefix";
  private static final String CON_TIMEOUT = "timeout.con";
  private static final String READ_TIMEOUT = "timeout.read";
  private static final String EXEC_TIMEOUT = "timeout.exec";
  private static final String LOG_ENABLED = "log.enable";
  private boolean logEnabled;
  private String urlPrefix;
  private Properties props;
  private CloseableHttpClient client;
  private int conTimeout = 10000;
  private int readTimeout = 10000;
  private int execTimeout = 10000;
  private volatile Criteria requestTimedout = new Criteria(false);

  @Override
  public void init() throws DBException {
    props = getProperties();
    urlPrefix = props.getProperty(URL_PREFIX, "http://127.0.0.1:80");
    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
    setupClient();
  }

  private void setupClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
    this.client = clientBuilder.setConnectionManagerShared(true).build();
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    int responseCode;
    String url = urlPrefix + "/" + table + "/read/" + key;
    try {
      responseCode = httpGet(url, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e, url, "GET");
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("GET Request: ").append(url)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values, int balance) {
    int responseCode;
    String url = urlPrefix + "/" + table + "/insert";
    try {
      responseCode = httpInsert(new HttpPost(url), key, values, balance);
    } catch (Exception e) {
      responseCode = handleExceptions(e, url, "POST");
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(url)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status transfer(String table, String outgoingKey, String incomingKey, int amount) {
    int responseCode;
    String url = urlPrefix + "/" + table + "/transfer";
    try {
      responseCode = httpTransfer(new HttpPost(url), outgoingKey, incomingKey, amount);
    } catch (Exception e) {
      responseCode = handleExceptions(e, url, "POST");
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(url)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status delete(String table, String endpoint) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status deleteAndTransfer(String table, String key, String incomingKey) {
    int responseCode;
    String url = urlPrefix + "/" + table + "/delete";
    try {
      responseCode = httpDelete(new HttpPost(url), key, incomingKey);
    } catch (Exception e) {
      responseCode = handleExceptions(e, url, "POST");
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(url)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    int responseCode;
    String url = urlPrefix + "/" + table + "/update";
    try {
      responseCode = httpUpdate(new HttpPost(url), key, values);
    } catch (Exception e) {
      responseCode = handleExceptions(e, url, "POST");
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(url)
          .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  // Maps HTTP status codes to YCSB status codes.
  private Status getStatus(int responseCode) {
    int rc = responseCode / 100;
    if (responseCode == 422) {
      return Status.TRANSACTION_FAILED;
    } else if (responseCode == 404) {
      return Status.NOT_FOUND;
    } else if (rc == 5 | rc == 4) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private int handleExceptions(Exception e, String url, String method) {
    if (logEnabled) {
      System.err.println(new StringBuilder(method).append(" Request: ").append(url).append(" | ")
          .append(e.getClass().getName()).append(" occured | Error message: ")
          .append(e.getMessage()).toString());
    }

    if (e instanceof ClientProtocolException) {
      return 400;
    }
    return 500;
  }

  // Connection is automatically released back in case of an exception.
  private int httpGet(String endpoint, Map<String, ByteIterator> result) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          client.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      result.put("response", new StringByteIterator(responseContent.toString()));
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    client.close();
    return responseCode;
  }

  private int httpInsert(
      HttpPost request, String key, Map<String, ByteIterator> values, int balance) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();

    JSONObject result = new JSONObject();
    result.put("id", key);
    result.put("balance", balance);
    JSONObject fields = new JSONObject();
    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      fields.put(entry.getKey(), entry.getValue().toString());
    }
    result.put("fields", fields);

    StringEntity entity = new StringEntity(result.toString());
    request.setEntity(entity);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    return httpExecute(request, timer);
  }

  private int httpTransfer(HttpPost request, String outgoingKey, String incomingKey, int amount) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();

    JSONObject result = new JSONObject();
    result.put("outgoing_id", outgoingKey);
    result.put("incoming_id", incomingKey);
    result.put("amount", amount);

    StringEntity entity = new StringEntity(result.toString());
    request.setEntity(entity);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    return httpExecute(request, timer);
  }

  private int httpDelete(HttpPost request, String key, String incomingKey) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();

    JSONObject result = new JSONObject();
    result.put("id", key);
    result.put("incoming_id", incomingKey);

    StringEntity entity = new StringEntity(result.toString());
    request.setEntity(entity);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    return httpExecute(request, timer);
  }

  private int httpUpdate(HttpPost request, String key, Map<String, ByteIterator> values) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();

    JSONObject result = new JSONObject();
    result.put("id", key);
    JSONObject fields = new JSONObject();
    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      fields.put(entry.getKey(), entry.getValue().toString());
    }
    result.put("fields", fields);

    StringEntity entity = new StringEntity(result.toString());
    request.setEntity(entity);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    return httpExecute(request, timer);

  }

  private int httpExecute(HttpEntityEnclosingRequestBase request, Thread timer) throws IOException {
    int responseCode = 200;

    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuffer responseContent = new StringBuffer();
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (requestTimedout.isSatisfied()) {
          // Must avoid memory leak.
          reader.close();
          stream.close();
          EntityUtils.consumeQuietly(responseEntity);
          response.close();
          client.close();
          throw new TimeoutException();
        }
        responseContent.append(line);
      }
      timer.interrupt();
      // Closing the input stream will trigger connection release.
      stream.close();
    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    client.close();
    return responseCode;
  }

  /**
   * Marks the input {@link Criteria} as satisfied when the input time has elapsed.
   */
  class Timer implements Runnable {

    private long timeout;
    private Criteria timedout;

    public Timer(long timeout, Criteria timedout) {
      this.timedout = timedout;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
        this.timedout.setIsSatisfied(true);
      } catch (InterruptedException e) {
        // Do nothing.
      }
    }

  }

  /**
   * Sets the flag when a criteria is fulfilled.
   */
  class Criteria {

    private boolean isSatisfied;

    public Criteria(boolean isSatisfied) {
      this.isSatisfied = isSatisfied;
    }

    public boolean isSatisfied() {
      return isSatisfied;
    }

    public void setIsSatisfied(boolean satisfied) {
      this.isSatisfied = satisfied;
    }

  }

  /**
   * Private exception class for execution timeout.
   */
  class TimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TimeoutException() {
      super("HTTP Request exceeded execution time limit.");
    }

  }

}
