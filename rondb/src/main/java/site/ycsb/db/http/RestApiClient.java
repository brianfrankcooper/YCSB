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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RonDB REST client wrapper.
 */
public final class RestApiClient extends DB {

  protected static Logger logger = LoggerFactory.getLogger(RestApiClient.class);

  private int readBatchSize = 1;
  private int numThreads;
  private String db;
  private String restServerIP;
  private int restServerPort;
  private String restAPIVersion;
  private String restServerURI;
  private MyHttpClient myHttpClient;
  private final int threadID;

  private static AtomicInteger maxID = new AtomicInteger(0);

  private static class Operation {
    private int opId;
    private String table;
    private String key;
    private Set<String> fields;
  }

  private Map<Integer/* batch id */, List<Operation>> operations = null;
  private Map<Integer, PKResponse> responses = null;
  private Properties properties;

  private List<CyclicBarrier> barriers;

  public RestApiClient(int threadID, Properties props) throws IOException {
    this.properties = props;
    this.threadID = threadID;
  }

  @Override
  public void init() throws DBException {
    //FIXME
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

    /*
     * Each batch has an equal amount of threads assigned to it
     * 9 threads, batch-size: 3
     * --> 3 threads/operations per batch
     * --> 3 threads synchronize their operations into a single batch
     */
    if (numThreads % readBatchSize != 0) {
      RonDBClient.getLogger().error("Wrong batch size. Total threads should be evenly " +
          "divisible by read batch size");
      System.exit(1);
    }

    /*
     * Essentially number of batches running at the same time
     * Since each batch can be supported by multiple threads,
     * some threads will need to share memory.
     */
    int numBarriers = numThreads / readBatchSize;
    operations = new ConcurrentHashMap<>();
    responses = new ConcurrentHashMap<>();

    barriers = new ArrayList<CyclicBarrier>(numBarriers);

    for (int i = 0; i < numBarriers; i++) {
      operations.put(i, Collections.synchronizedList(new ArrayList<>(readBatchSize)));
      barriers.add(new CyclicBarrier(readBatchSize, new Batcher(i)));
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
    /*
     * Each client/thread is always assigned to the same batch/barrier id.
     * We check here which barrier this is, so that we can synchronize
     * the clients.
     */
    int batchID = threadID / readBatchSize; // this should round down
    int barrierID = batchID; // barrier is simply a batch that is being processed and is shared by threads

    Operation op = new Operation();
    op.opId = maxID.incrementAndGet();
    op.key = key;
    op.table = table;
    op.fields = fields;

    List<Operation> ops = operations.get(batchID);
    ops.add(op);

    try {
      // This synchronizes the threads; A barrier has a runnable action, which will be
      // executed here
      barriers.get(barrierID).await();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      PKResponse response = responses.get(op.opId);
      if (response != null) {
        for (String column : fields) {
          String val = response.getData(op.opId, column);
          result.put(
              column,
              new ByteArrayByteIterator(val.getBytes(), 0, val.length()));
        }
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } finally {
      responses.remove(op.opId);
    }
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          HashMap<String, HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("Batch reads are not yet supported");
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

  public void notifyAllBarriers() {
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
          RonDBClient.getLogger().trace("Error " + e);
        }
      } else {
        try {
          String responseStr = pkRESTCall();
          processPkResponse(responseStr);
        } catch (Exception e) {
          RonDBClient.getLogger().warn("Error " + e);
        }
      }
    }

    private String pkRESTCall() throws Exception {
      String jsonReq = null;
      String table = null;
      List<Operation> ops;

      ops = operations.get(batchID);
      try {
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
          BatchSubOperation subOperation = new BatchSubOperation(
              db + "/" + ops.get(i).table + "/pk-read",
              pkRequest);
          batch.addSubOperation(subOperation);
        }

        jsonReq = batch.toString();
        // System.out.println(jsonReq);

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

  @Override
  public void cleanup() throws DBException {
  }
}
