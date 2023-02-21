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
package site.ycsb.db.grpc;

import com.rondb.grpcserver.*;
import com.rondb.grpcserver.RonDBRESTGrpc.RonDBRESTBlockingStub;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.db.ConfigKeys;
import site.ycsb.db.clusterj.table.UserTableHelper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RonDB res client wrapper.
 */
public final class GrpcClient extends DB {
  // TODO: Add API key; Place into HTTP header under "X-API-KEY"
  protected static Logger logger = LoggerFactory.getLogger(GrpcClient.class);

  private static Object lock = new Object();
  private String databaseName;
  private String grpcServerIP;
  private int grpcServerPort;
  private final Properties properties;
  private final int threadID;

  private PKReadRequestProto.Builder basePkReadBuilder;
  private ReadColumnProto.Builder readFieldsBuilder;

  private static ManagedChannel channel;
  // TODO: Use AsyncStub instead to use it in parallel
  private static RonDBRESTBlockingStub blockingStub;

  private static AtomicInteger maxID = new AtomicInteger(0);

  public GrpcClient(int threadID, Properties props) throws IOException {
    this.threadID = threadID;
    this.properties = props;
  }

  public void init() throws DBException {
    try {
      databaseName = properties.getProperty(ConfigKeys.SCHEMA_KEY, ConfigKeys.SCHEMA_DEFAULT);

      // In case we're e.g. using container names: https://github.com/grpc/grpc-java/issues/4564#issuecomment-396817986
      String grpcServerHostname = properties.getProperty(ConfigKeys.RONDB_REST_SERVER_IP_KEY,
          ConfigKeys.RONDB_REST_SERVER_IP_DEFAULT);
      java.net.InetAddress inetAddress = java.net.InetAddress.getByName(grpcServerHostname);
      grpcServerIP = inetAddress.getHostAddress();

      grpcServerPort = Integer.parseInt(properties.getProperty(ConfigKeys.RONDB_GRPC_SERVER_PORT_KEY,
          Integer.toString(ConfigKeys.RONDB_GRPC_SERVER_PORT_DEFAULT)));
      String grpcServerAddress = grpcServerIP + ":" + grpcServerPort;

      basePkReadBuilder = PKReadRequestProto.newBuilder().setAPIKey("").setDB(databaseName);
      synchronized (lock) {
        if (channel == null) {
          channel = Grpc.newChannelBuilder(grpcServerAddress, InsecureChannelCredentials.create()).build();
        }
        if (blockingStub == null) {
          blockingStub = RonDBRESTGrpc.newBlockingStub(channel);
        }
      }
      test();
    } catch (IOException e){
      logger.error(e.getMessage(), e);
      throw new DBException(e);
    }
  }

  /**
   * This tests the REST client connection.
   */
  private void test() throws DBException {
    logger.info("Running gRPC test against test endpoint");
    try {
      StatResponseProto response = blockingStub.stat(StatRequestProto.newBuilder().build());
      if (response != null) {
        logger.info("response for stat endpoint: " + response.toString());
      } else {
        logger.error("response is null for Stat endpoint!");
        System.exit(1);
      }
    } catch (StatusRuntimeException e) {
      logger.warn("RPC failed: {0}", e.getStatus());
      System.exit(1);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {

    String operationID = Integer.toString(maxID.incrementAndGet());
    FilterProto filter = FilterProto.newBuilder().setColumn(UserTableHelper.KEY).setValue(key).build();
    PKReadRequestProto.Builder pkReadBuilder = basePkReadBuilder.setOperationID(operationID)
        .setTable(table)
        .addFilters(filter);
    for (String field : fields) {
      pkReadBuilder = pkReadBuilder.addReadColumns(readFieldsBuilder.setColumn(field).build());
    }

    logger.warn("Making pkReadRequest with operation number : " + operationID);
    PKReadResponseProto response = PKReadResponseProto.newBuilder().build();
    try {
      PKReadRequestProto pkRead = pkReadBuilder.build();
      response = blockingStub.pKRead(pkRead);
    } catch (StatusRuntimeException e) {
      e.printStackTrace();
      System.exit(1);
    }

    if (response == null || !response.isInitialized()) {
      logger.error("gRPC is empty");
      return Status.NOT_FOUND;
    } else {
      logger.warn("response for pkRead: " + response.toString());
    }

    Map<String, ColumnValueProto> dataMap = response.getDataMap();
    byte[] value;
    for (Map.Entry<String, ColumnValueProto> entry : dataMap.entrySet()) {
      value = entry.getValue().toByteArray();
      result.put(
          entry.getKey(),
          new ByteArrayByteIterator(value, 0, value.length));
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String msg = "Scan is not supported by GRPC API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String msg = "Update is not supported by GRPC API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String msg = "Insert is not supported by GRPC API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public Status delete(String table, String key) {
    String msg = "Delete is not supported by GRPC API";
    RuntimeException up = new UnsupportedOperationException(msg);
    throw up;
  }

  @Override
  public void cleanup() throws DBException {

  }
}
