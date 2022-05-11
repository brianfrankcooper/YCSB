/*
 * Copyright (c) 2021, Yahoo!, Inc. All rights reserved.
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
package site.ycsb.db;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.Status;
import site.ycsb.db.table.UserTableHelper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

/**
 * RonDB res client wrapper.
 */
public final class RonDBRestClient {

  private static final String RONDB_USE_REST_API = "rondb.use.rest.api";
  private static final String RONDB_REST_API_BATCH_SIZE = "rondb.rest.api.batch.size";
  private static final String RONDB_REST_SERVER_IP = "rondb.rest.server.ip";
  private static final String RONDB_REST_SERVER_PORT = "rondb.rest.server.port";
  private static final String RONDB_REST_API_VERSION = "rondb.rest.api.version";

  private boolean useRESTAPI = false;
  private int readBatchSize = 1;
  private String restServerIP = "localhost";
  private int restServerPort = 5000;
  private String restAPIVersion = "0.1.0";
  private CloseableHttpClient httpClient;
  private String restServerURI;
  private int numThreads = 1;
  private String db;

  private static RonDBRestClient restClient;

  private static class Operation {
    private String table;
    private String key;
    private Set<String> fields;
  }

  private Map<Integer /*batch id*/, List<Operation>> operations = null;
  private Map<String/*PK as op Id*/, LinkedHashMap<String/*PK as op Id*/, Object>> responses = null;

  private List<CyclicBarrier> barriers;


  private RonDBRestClient() {
  }

  /**
   * Initialize the client.
   */
  public static synchronized void initialize(Properties properties) {

    if (restClient == null) {
      restClient = new RonDBRestClient();
      restClient.setupRestAPIParams(properties);
    }
  }

  /**
   * Get the client.
   */
  public static RonDBRestClient getClient() {
    if (restClient == null) {
      throw new IllegalStateException("Rest client not initialized");
    }
    return restClient;
  }

  /**
   * Set up parameters.
   */
  private void setupRestAPIParams(Properties props) {
    try {
      String pstr = props.getProperty(RONDB_USE_REST_API, "false");
      useRESTAPI = Boolean.parseBoolean(pstr);

      if (useRESTAPI) {
        db = props.getProperty(RonDBConnection.SCHEMA, "ycsb");

        pstr = props.getProperty(RONDB_REST_API_BATCH_SIZE, "1");
        readBatchSize = Integer.parseInt(pstr);

        restServerIP = props.getProperty(RONDB_REST_SERVER_IP, "localhost");

        pstr = props.getProperty(RONDB_REST_SERVER_PORT, "5000");
        restServerPort = Integer.parseInt(pstr);

        restAPIVersion = props.getProperty(RONDB_REST_API_VERSION, "0.1.0");

        restServerURI = "http://" + restServerIP + ":" + restServerPort + "/" + restAPIVersion;

        RequestConfig.Builder requestBuilder = RequestConfig.custom();
        requestBuilder = requestBuilder.setConnectTimeout(10 * 1000);
        requestBuilder = requestBuilder.setConnectionRequestTimeout(10 * 1000);
        requestBuilder = requestBuilder.setSocketTimeout(10 * 1000);
        HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
        this.httpClient = clientBuilder.setConnectionManagerShared(true).build();

        pstr = props.getProperty(Client.THREAD_COUNT_PROPERTY, "1");
        numThreads = Integer.parseInt(pstr);

        if (numThreads % readBatchSize != 0) {
          RonDBClient.logger.error("Wrong batch size. Total threads should be evenly " +
              "divisible by read batch size");
          System.exit(1);
        }

        int numBarriers = numThreads / readBatchSize;
        operations = new ConcurrentHashMap<>();
        responses = new HashMap<>();

        barriers = new ArrayList(numBarriers);

        for (int i = 0; i < numBarriers; i++) {
          operations.put(i, Collections.synchronizedList(new ArrayList<Operation>(readBatchSize)));
          barriers.add(new CyclicBarrier(readBatchSize, new Batcher(i)));
        }
      }
    } catch (Exception e) {
      RonDBClient.logger.error("Unable to parse parameters for REST SERVER. " + e);
      System.exit(1);
    }

    try {
      if (useRESTAPI) {
        // test connection
        test();
      }
    } catch (Exception e) {
      RonDBClient.logger.error("Unable to connect to REST server. " + e);
      System.exit(1);
    }
  }

  /**
   * Test the rest client.
   */
  private void test() throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpGet req = new HttpGet(restServerURI + "/stat");
      response = httpClient.execute(req);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  /**
   * Is using rest client.
   */
  public static boolean useRESTAPI() {
    return getClient().useRESTAPI;
  }

  public void notifyAllBarriers() {
//    synchronized (this) {
//      for (int i = 0; i < barriers.size(); i++) {
//        barriers.get(i).
//      }
//    }
  }

  public Status read(Integer threadID, String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) throws InterruptedException, BrokenBarrierException {

    int batchID = threadID / readBatchSize;
    int barrierID = batchID;

    Operation op = new Operation();
    op.key = key;
    op.table = table;
    op.fields = fields;

    List<Operation> ops = operations.get(batchID);
    ops.add(op);

    try {
      barriers.get(barrierID).await();
//      barriers.get(barrierID).await(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      //
      e.printStackTrace();
    }

    try {
      LinkedHashMap<String, Object> response = responses.get(key);
      if (response != null) {
        for (String column : fields) {
          String val = (String) response.get(column);
          result.put(column, new ByteArrayByteIterator(val.getBytes(), 0, val.length()));
        }
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } finally {
      responses.remove(key);
    }
  }


  private class Batcher implements Runnable {
    private int batchID;

    Batcher(int id) {
      this.batchID = id;
    }

    @Override
    public void run() {
      if (readBatchSize > 1) {
        String responseStr = batchRESTCall();
        processBatchResponse(responseStr);
      } else {
        String responseStr = pkRESTCall();
        processPkResponse(responseStr);
      }
    }

    private String pkRESTCall() {
      String jsonReq = null;
      String table = null;
      List<Operation> ops;
      try {
        ops = operations.get(batchID);
        //System.out.println("Batch ID: " + batchID + "   Length is " + ops.size());

        if (ops.size() != 1) {
          throw new IllegalStateException("Batch size is expected to be 1");
        }

        Operation op = ops.get(0);

        if (table == null) {
          table = op.table;
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode pkReq = mapper.createObjectNode();

        ObjectNode[] filtersArr = new ObjectNode[1];
        filtersArr[0] = new ObjectMapper().createObjectNode();
        filtersArr[0].put("column", UserTableHelper.KEY);
        filtersArr[0].put("value", op.key);

        ArrayNode filtersNode = mapper.createArrayNode();
        filtersNode.addAll(Arrays.asList(filtersArr));

        pkReq.set("filters", filtersNode);

        ObjectNode[] readCols = new ObjectNode[op.fields.size()];
        for (int i = 0; i < op.fields.size(); i++) {
          String colName = (String) op.fields.toArray()[i];
          readCols[i] = new ObjectMapper().createObjectNode();
          readCols[i].put("column", colName);
        }

        ArrayNode readColsNode = mapper.createArrayNode();
        readColsNode.addAll(Arrays.asList(readCols));

        pkReq.set("readColumns", readColsNode);
        pkReq.put("operationId", op.key);
        jsonReq = mapper.writer().writeValueAsString(pkReq);
        //System.out.println(jsonReq);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      try {
        String uri = restServerURI + "/" + db + "/" + table + "/pk-read";
        HttpPost req = new HttpPost(uri);
        StringEntity stringEntity = new StringEntity(jsonReq);
        req.setEntity(stringEntity);
        return httpClient.execute(req, new MyResponseHandler());
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      } finally {
        ops.clear();
      }
    }

    private String batchRESTCall() {
      String jsonReq = null;
      String table = null;
      List<Operation> ops;
      try {

        ops = operations.get(batchID);
        //System.out.println("Batch ID: " + batchID + "   Length is " + ops.size());

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode[] reqObjs = new ObjectNode[ops.size()];
        for (int i = 0; i < reqObjs.length; i++) {
          reqObjs[i] = mapper.createObjectNode();
        }

        for (int i = 0; i < ops.size(); i++) {
          Operation op = ops.get(i);
          if (table == null) {
            table = op.table;
          }

          ObjectNode[] filtersArr = new ObjectNode[1];

          filtersArr[0] = new ObjectMapper().createObjectNode();
          filtersArr[0].put("column", UserTableHelper.KEY);
          filtersArr[0].put("value", op.key);
          ArrayNode filtersNode = mapper.createArrayNode();
          filtersNode.addAll(Arrays.asList(filtersArr));


          ObjectNode pkReq = mapper.createObjectNode();
          pkReq.set("filters", filtersNode);

          ObjectNode[] readCols = new ObjectNode[op.fields.size()];
          for (int f = 0; f < op.fields.size(); f++) {
            String colName = (String) op.fields.toArray()[f];
            readCols[f] = new ObjectMapper().createObjectNode();
            readCols[f].put("column", colName);
          }

          ArrayNode readColsNode = mapper.createArrayNode();
          readColsNode.addAll(Arrays.asList(readCols));

          pkReq.set("readColumns", readColsNode);
          pkReq.put("operationId", op.key);
          reqObjs[i].put("method", "POST");
          reqObjs[i].put("relative-url", db + "/" + ops.get(i).table + "/pk-read");
          reqObjs[i].set("body", pkReq);
        }

        ArrayNode batchReqArr = mapper.createArrayNode();
        batchReqArr.addAll(Arrays.asList(reqObjs));
        ObjectNode batchReq = mapper.createObjectNode();
        batchReq.set("operations", batchReqArr);

        jsonReq = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(batchReq);
        //System.out.println(jsonReq);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      try {
        HttpPost req = new HttpPost(restServerURI + "/batch");
        StringEntity stringEntity = new StringEntity(jsonReq);
        req.setEntity(stringEntity);
        return httpClient.execute(req, new MyResponseHandler());
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      } finally {
        ops.clear();
      }

    }

    private void processPkResponse(String responseStr) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        LinkedHashMap<String, Object> bodyMap = (LinkedHashMap<String, Object>)
            mapper.readValue(responseStr, Object.class);
        String opId = (String) bodyMap.get("operationId");
        LinkedHashMap<String, Object> dataMap = (LinkedHashMap<String, Object>) bodyMap.get("data");
        responses.put(opId, dataMap);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void processBatchResponse(String responseStr) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        Object[] myOps = mapper.readValue(responseStr, Object[].class);
        for (Object op : myOps) {

          // it contains code and body
          LinkedHashMap<String, Object> opMap = (LinkedHashMap<String, Object>) op;

          // body
          LinkedHashMap<String, Object> bodyMap = (LinkedHashMap<String, Object>) opMap.get("body");

          String opId = (String) bodyMap.get("operationId");
          // row data
          LinkedHashMap<String, Object> dataMap = (LinkedHashMap<String, Object>) bodyMap.get("data");

          responses.put(opId, dataMap);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  final class MyResponseHandler implements ResponseHandler<String> {
    @Override
    public String handleResponse(HttpResponse response) throws ClientProtocolException,
        IOException {
      //Get the status of the response
      int status = response.getStatusLine().getStatusCode();
      if (status >= 200 && status < 300) {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
          return "";
        } else {
          return EntityUtils.toString(entity);
        }
      } else {
        throw new IOException(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
      }
    }
  }
}
