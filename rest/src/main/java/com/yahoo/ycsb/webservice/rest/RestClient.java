package com.yahoo.ycsb.webservice.rest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Class responsible for making web service requests for benchmarking purpose.
 * Using Apache HttpClient over standard Java HTTP API as this is more flexible
 * and provides better functionality. For example HttpClient can automatically
 * handle Redirects and Proxy Authentication which the standard Java API don't.
 * 
 * @author shivam.maharshi
 */
public class RestClient extends DB {

  private static final String URL_PREFIX = "url.prefix";
  private static final String CON_TIMEOUT = "timeout.con";
  private static final String READ_TIMEOUT = "timeout.read";
  private static final String EXEC_TIMEOUT = "timeout.exec";
  private static final String LOG_ENABLED = "log.enable";
  private static final String HEADERS = "headers";
  private static final String COMPRESSED_RESPONSE = "response.compression";
  private static boolean compressedResponse;
  private static boolean logEnabled;
  private String urlPrefix;
  private Properties props;
  private String[] headers;
  private CloseableHttpClient client;
  private static int conTimeout = 10000;
  private static int readTimeout = 10000;
  private static int execTimeout = 10000;
  private volatile Criteria requestTimedout = new Criteria(false);

  @Override
  public void init() throws DBException {
    props = getProperties();
    urlPrefix = props.getProperty(URL_PREFIX, "127.0.0.1:8080");
    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
    compressedResponse = Boolean.valueOf(props.getProperty(COMPRESSED_RESPONSE, "false").trim());
    headers = props.getProperty(HEADERS, "Accept */* Content-Type application/xml user-agent Mozilla/5.0 ").trim()
          .split(" ");
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
  public Status read(String table, String endpoint, Set<String> fields, HashMap<String, ByteIterator> result) {
    int responseCode;
    try {
      responseCode = httpGet(urlPrefix + endpoint, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e);
    }
    if (logEnabled) {
      System.out.println(new StringBuilder("GET Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status insert(String table, String endpoint, HashMap<String, ByteIterator> values) {
    int responseCode;
    try {
      responseCode = httpPost(urlPrefix + endpoint, values.get("data").toString());
    } catch (Exception e) {
      responseCode = handleExceptions(e);
    }
    if (logEnabled) {
      System.out.println(new StringBuilder("POST Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status delete(String table, String endpoint) {
    int responseCode;
    try {
      responseCode = httpDelete(urlPrefix + endpoint);
    } catch (Exception e) {
      responseCode = handleExceptions(e);
    }
    if (logEnabled) {
      System.out.println(new StringBuilder("DELETE Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  // Maps HTTP status codes to YCSB status codes.
  private Status getStatus(int responseCode) {
    int rc = responseCode / 100;
    if (responseCode == 400) {
      return Status.BAD_REQUEST;
    } else if (responseCode == 403) {
      return Status.FORBIDDEN;
    } else if (responseCode == 404) {
      return Status.NOT_FOUND;
    } else if (responseCode == 501) {
      return Status.NOT_IMPLEMENTED;
    } else if (responseCode == 503) {
      return Status.SERVICE_UNAVAILABLE;
    } else if (rc == 5) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private int handleExceptions(Exception e) {
    if (e instanceof ClientProtocolException) {
      return 400;
    }
    return 500;
  }

  // Connection is automatically released back in case of an exception.
  private int httpGet(String endpoint, HashMap<String, ByteIterator> result) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      /*
       * TODO: Gzip Compression must be supported in the future. Header[]
       * header = response.getAllHeaders();
       * if(response.getHeaders("Content-Encoding")[0].getValue().contains
       * ("gzip")) stream = new GZIPInputStream(stream);
       */
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

  // Connection is automatically released back in case of an exception.
  private int httpPost(String endpoint, String postData) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpPost request = new HttpPost(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(postData.getBytes()),
          ContentType.APPLICATION_FORM_URLENCODED);
    reqEntity.setChunked(true);
    request.setEntity(reqEntity);
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
    if (responseEntity != null) {
      InputStream stream = responseEntity.getContent();
      if (compressedResponse) {
        stream = new GZIPInputStream(stream); 
      }
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

    // Connection is automatically released back in case of an exception.
  private int httpDelete(String endpoint) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpDelete request = new HttpDelete(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    response.close();
    client.close();
    return responseCode;
  }

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
        // Do nothing. Kept just to remove Check-style build error.
        byte a = 0;
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

  }

}