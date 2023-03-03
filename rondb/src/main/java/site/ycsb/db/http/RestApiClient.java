/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
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

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 * RonDB client binding for YCSB.
 */
package site.ycsb.db.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.db.ConfigKeys;
import site.ycsb.db.RonDBClient;
import site.ycsb.db.clusterj.table.UserTableHelper;
import site.ycsb.db.http.ds.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * RonDB REST client wrapper.
 */
public final class RestApiClient extends DB {

  protected static Logger logger = LoggerFactory.getLogger(RestApiClient.class);

  private int numThreads;
  private String db;
  private String restServerIP;
  private int restServerPort;
  private String restAPIVersion;
  private String restServerURI;
  private MyHttpClient myHttpClient;
  private final int threadID;

  private Properties properties;

  public RestApiClient(int threadID, Properties props) throws IOException {
    this.properties = props;
    this.threadID = threadID;
  }

  @Override
  public void init() throws DBException {
    numThreads = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    db = properties.getProperty(ConfigKeys.SCHEMA_KEY, ConfigKeys.SCHEMA_DEFAULT);
    restServerIP = properties.getProperty(ConfigKeys.RONDB_REST_SERVER_IP_KEY,
        ConfigKeys.RONDB_REST_SERVER_IP_DEFAULT);
    restServerPort = Integer.parseInt(properties.getProperty(ConfigKeys.RONDB_REST_SERVER_PORT_KEY,
        Integer.toString(ConfigKeys.RONDB_REST_SERVER_PORT_DEFAULT)));
    restAPIVersion = properties.getProperty(ConfigKeys.RONDB_REST_API_VERSION_KEY,
        ConfigKeys.RONDB_REST_API_VERSION_DEFAULT);
    restServerURI = "http://" + restServerIP + ":" + restServerPort + "/" + restAPIVersion;
    boolean async =
        Boolean.parseBoolean(properties.getProperty(ConfigKeys.RONDB_REST_API_USE_ASYNC_REQUESTS_KEY,
            Boolean.toString(ConfigKeys.RONDB_REST_API_USE_ASYNC_REQUESTS_DEFAULT)));

    try {
      if (async) {
        myHttpClient = new MyHttpClientAsync(numThreads);
      } else {
        myHttpClient = new MyHttpClientSync();
      }
    } catch (IOReactorException e) {
      logger.error(e.getMessage(), e);
      throw new DBException(e);
    }

    try {
      test();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new DBException(e);
    }
  }

  /**
   * This tests the REST client connection.
   */
  private void test() throws IOException {
    CloseableHttpResponse response = null;
    String url = restServerURI + "/stat";
    RonDBClient.getLogger().info("Running test against url " + url);
    try {
      HttpGet req = new HttpGet(url);
      myHttpClient.execute(req);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String response;
    try {
      PKRequest pkReq = new PKRequest(key);
      pkReq.addFilter(UserTableHelper.KEY, key);

      for (String colName : fields) {
        pkReq.addReadColumn(colName);
      }

      String jsonReq = pkReq.toString();
      String uri = restServerURI + "/" + db + "/" + table + "/pk-read";
      HttpPost req = new HttpPost(uri);
      StringEntity stringEntity = new StringEntity(jsonReq);
      req.setEntity(stringEntity);
      response = myHttpClient.execute(req);
      return processPkResponse(response, fields, result);
    } catch (MyHttpException | UnsupportedEncodingException e) {
      RonDBClient.getLogger().warn("Error " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          HashMap<String, HashMap<String, ByteIterator>> result) {
    String jsonReq;
    BatchRequest batch = new BatchRequest();
    try {
      for (int i = 0; i < keys.size(); i++) {
        String key = keys.get(i);
        Set<String> readColumns = fields.get(i);

        PKRequest pkRequest = new PKRequest(key/*op id*/);
        pkRequest.addFilter(UserTableHelper.KEY, key);
        for (String columnName : readColumns) {
          pkRequest.addReadColumn(columnName);
        }
        BatchSubOperation subOperation = new BatchSubOperation(
            db + "/" + table + "/pk-read",
            pkRequest);
        batch.addSubOperation(subOperation);
      }

      jsonReq = batch.toString();
      // System.out.println(jsonReq);

      HttpPost req = new HttpPost(restServerURI + "/batch");
      StringEntity stringEntity = new StringEntity(jsonReq);
      req.setEntity(stringEntity);
      String response = myHttpClient.execute(req);
      return processBatchResponse(response, keys, fields, result);
    } catch (MyHttpException | UnsupportedEncodingException e) {
      RonDBClient.getLogger().warn("Error " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String msg = "Scan is not supported by REST API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String msg = "Update is not supported by REST API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String msg = "Insert is not supported by REST API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status delete(String table, String key) {
    String msg = "Delete is not supported by REST API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  private Status processPkResponse(String responseStr, Set<String> fields, Map<String,
      ByteIterator> result) {

    PKResponse response = new PKResponse(responseStr);
    if (response != null) {
      for (String column : fields) {
        String val = response.getData(response.getOpId(), column);
        result.put(column, new ByteArrayByteIterator(val.getBytes(), 0, val.length()));
      }
      return Status.OK;
    } else {
      return Status.NOT_FOUND;
    }
  }

  private Status processBatchResponse(String responseStr, List<String> keys,
                                      List<Set<String>> fields,
                                      HashMap<String, HashMap<String, ByteIterator>> results) {
    boolean allFound = true;
    BatchResponse batchResponse = new BatchResponse(responseStr);
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      Set<String> projectionFields = fields.get(i);
      HashMap<String, ByteIterator> result = results.get(key);
      PKResponse subPkResponse = batchResponse.getSubResponses(key);
      if (subPkResponse != null) {
        for (String column : projectionFields) {
          String val = subPkResponse.getData(key, column);
          result.put(column, new ByteArrayByteIterator(val.getBytes(), 0, val.length()));
        }
      } else {
        allFound = false;
      }
    }
    if (allFound) {
      return Status.OK;
    } else {
      return Status.NOT_FOUND;
    }
  }

  @Override
  public void cleanup() throws DBException {
  }
}
