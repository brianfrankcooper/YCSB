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
package site.ycsb.db.rest;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.Status;
import site.ycsb.db.RonDBClient;
import site.ycsb.db.RonDBConnection;
import site.ycsb.db.rest.ds.BatchRequest;
import site.ycsb.db.rest.ds.BatchResponse;
import site.ycsb.db.rest.ds.BatchSubOperation;
import site.ycsb.db.rest.ds.MyHttpClientAsync;
import site.ycsb.db.rest.ds.MyHttpClientSync;
import site.ycsb.db.rest.ds.MyHttpClinet;
import site.ycsb.db.rest.ds.PKRequest;
import site.ycsb.db.rest.ds.PKResponse;
import site.ycsb.db.table.UserTableHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RonDB res client wrapper.
 */
public final class RonDBRestClient {

  private static final String RONDB_USE_REST_API = "rondb.use.rest.api";
  private static final String RONDB_REST_API_BATCH_SIZE = "rondb.rest.api.batch.size";
  private static final String RONDB_REST_SERVER_IP = "rondb.rest.server.ip";
  private static final String RONDB_REST_SERVER_PORT = "rondb.rest.server.port";
  private static final String RONDB_REST_API_VERSION = "rondb.rest.api.version";
  private static final String RONDB_REST_API_USE_ASYNC_REQUESTS = "rondb.rest.api.use.async.requests";

  private boolean useRESTAPI = false;
  private int readBatchSize = 1;
  private String restServerIP = "localhost";
  private int restServerPort = 5000;
  private String restAPIVersion = "0.1.0";
  private String restServerURI;
  private int numThreads = 1;
  private String db;
  private MyHttpClinet myHttpClient;

  private static RonDBRestClient restClient;
  private static AtomicInteger maxID = new AtomicInteger(0);

  private static class Operation {
    private int opId;
    private String table;
    private String key;
    private Set<String> fields;
  }

  private Map<Integer /*batch id*/, List<Operation>> operations = null;
  private Map<Integer, PKResponse> responses = null;

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
        pstr = props.getProperty(RONDB_REST_API_BATCH_SIZE, "1");
        readBatchSize = Integer.parseInt(pstr);

        pstr = props.getProperty(Client.THREAD_COUNT_PROPERTY, "1");
        numThreads = Integer.parseInt(pstr);

        db = props.getProperty(RonDBConnection.SCHEMA, "ycsb");

        pstr = props.getProperty(RONDB_REST_API_BATCH_SIZE, "1");
        readBatchSize = Integer.parseInt(pstr);

        restServerIP = props.getProperty(RONDB_REST_SERVER_IP, "localhost");

        pstr = props.getProperty(RONDB_REST_SERVER_PORT, "5000");
        restServerPort = Integer.parseInt(pstr);

        restAPIVersion = props.getProperty(RONDB_REST_API_VERSION, "0.1.0");

        restServerURI = "http://" + restServerIP + ":" + restServerPort + "/" + restAPIVersion;

        pstr = props.getProperty(Client.THREAD_COUNT_PROPERTY, "1");
        numThreads = Integer.parseInt(pstr);

        pstr = props.getProperty(RONDB_REST_API_USE_ASYNC_REQUESTS, "false");
        boolean async = Boolean.parseBoolean(pstr);

        if(async) {
          myHttpClient = new MyHttpClientAsync(numThreads);
        } else{
          myHttpClient = new MyHttpClientSync();
        }

        if (numThreads % readBatchSize != 0) {
          RonDBClient.getLogger().error("Wrong batch size. Total threads should be evenly " +
              "divisible by read batch size");
          System.exit(1);
        }

        int numBarriers = numThreads / readBatchSize;
        operations = new ConcurrentHashMap<>();
        responses = new ConcurrentHashMap<>();

        barriers = new ArrayList(numBarriers);

        for (int i = 0; i < numBarriers; i++) {
          operations.put(i, Collections.synchronizedList(new ArrayList<Operation>(readBatchSize)));
          barriers.add(new CyclicBarrier(readBatchSize, new Batcher(i)));
        }
      }
    } catch (Exception e) {
      RonDBClient.getLogger().error("Unable to parse parameters for REST SERVER. " + e);
      System.exit(1);
    }

    try {
      if (useRESTAPI) {
        // test connection
        test();
      }
    } catch (Exception e) {
      RonDBClient.getLogger().error("Unable to connect to REST server. " + e);
      e.printStackTrace();
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
      myHttpClient.execute(req);
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
    op.opId = maxID.incrementAndGet();
    op.key = key;
    op.table = table;
    op.fields = fields;

    List<Operation> ops = operations.get(batchID);
    ops.add(op);

    try {
      barriers.get(barrierID).await();
      //barriers.get(barrierID).await(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      PKResponse response = responses.get(op.opId);
      if (response != null) {
        for (String column : fields) {
          String val = response.getData(op.opId, column);
          result.put(column, new ByteArrayByteIterator(val.getBytes(), 0, val.length()));
        }
        return Status.OK;
      } else {
        //System.out.println("NOT FOUND");
        return Status.NOT_FOUND;
      }
    } finally {
      responses.remove(op.opId);
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
        try {
          String responseStr = batchRESTCall();
          processBatchResponse(responseStr);
        } catch (Exception e) {
          RonDBClient.getLogger().trace("Error "+e);
        }
      } else {
        try {
          String responseStr = pkRESTCall();
          processPkResponse(responseStr);
        } catch (Exception e) {
          RonDBClient.getLogger().trace("Error "+e);
        }
      }
    }

    private String pkRESTCall() throws Exception {
      String jsonReq = null;
      String table = null;
      List<Operation> ops;

      ops = operations.get(batchID);
      try {
        //System.out.println("Batch ID: " + batchID + "   Length is " + ops.size());
        if (ops.size() != 1) {
          throw new IllegalStateException("Batch size is expected to be 1");
        }
        Operation op = ops.get(0);

        if (table == null) {
          table = op.table;
        }

        PKRequest pkReq = new PKRequest(Integer.toString(op.opId));
        pkReq.addFilter(UserTableHelper.KEY, op.key);

        for (int i = 0; i < op.fields.size(); i++) {
          String colName = (String) op.fields.toArray()[i];
          pkReq.addReadColumn(colName);
        }
        jsonReq = pkReq.toString();
        //System.out.println(jsonReq);

        String uri = restServerURI + "/" + db + "/" + table + "/pk-read";
        HttpPost req = new HttpPost(uri);
        StringEntity stringEntity = new StringEntity(jsonReq);
        req.setEntity(stringEntity);
        return myHttpClient.execute(req);
      } finally {
        ops.clear();
      }
    }

    private String batchRESTCall() throws Exception {
      String jsonReq = null;
      String table = null;
      List<Operation> ops;
      ops = operations.get(batchID);

      BatchRequest batch = new BatchRequest();
      try {
        for (int i = 0; i < ops.size(); i++) {
          Operation op = ops.get(i);
          if (table == null) {
            table = op.table;
          }

          PKRequest pkRequest = new PKRequest(Integer.toString(op.opId));
          pkRequest.addFilter(UserTableHelper.KEY, op.key);

          for (int f = 0; f < op.fields.size(); f++) {
            String colName = (String) op.fields.toArray()[f];
            pkRequest.addReadColumn(colName);
          }
          BatchSubOperation subOperation = new BatchSubOperation(db + "/" + ops.get(i).table +
              "/pk-read", pkRequest);
          batch.addSubOperation(subOperation);
        }

        jsonReq = batch.toString();
        //System.out.println(jsonReq);

        HttpPost req = new HttpPost(restServerURI + "/batch");
        StringEntity stringEntity = new StringEntity(jsonReq);
        req.setEntity(stringEntity);
        return myHttpClient.execute(req);
      } finally {
        ops.clear();
      }
    }

    private void processPkResponse(String responseStr) {
      PKResponse response = new PKResponse(responseStr);
      responses.put(response.getOpId(), response);
    }

    private void processBatchResponse(String responseStr) {
      BatchResponse batchResponse = new BatchResponse(responseStr);
      for (PKResponse pkResponse : batchResponse.getSubResponses()) {
        responses.put(pkResponse.getOpId(), pkResponse);
      }
    }
  }
}
