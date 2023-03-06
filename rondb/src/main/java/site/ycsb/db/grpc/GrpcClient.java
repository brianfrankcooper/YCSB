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

import com.rondb.grpcserver.RonDBGrpcProto;
import com.rondb.grpcserver.RonDBGrpcProto.ColumnValueProto;
import com.rondb.grpcserver.RonDBGrpcProto.FilterProto;
import com.rondb.grpcserver.RonDBGrpcProto.PKReadRequestProto;
import com.rondb.grpcserver.RonDBGrpcProto.ReadColumnProto;
import com.rondb.grpcserver.RonDBRESTGrpc;
import com.rondb.grpcserver.RonDBRESTGrpc.RonDBRESTBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

  private ManagedChannel channel;
  // TODO: Use AsyncStub instead to use it in parallel
  private RonDBRESTBlockingStub blockingStub;
  private static AtomicInteger maxID = new AtomicInteger(0);

  public GrpcClient(int threadID, Properties props) throws IOException {
    this.threadID = threadID;
    this.properties = props;
  }

  public void init() throws DBException {
    try {
      databaseName = properties.getProperty(ConfigKeys.SCHEMA_KEY, ConfigKeys.SCHEMA_DEFAULT);

      // In case we're e.g. using container names:
      // https://github.com/grpc/grpc-java/issues/4564#issuecomment-396817986
      String grpcServerHostname = properties.getProperty(ConfigKeys.RONDB_REST_SERVER_IP_KEY,
          ConfigKeys.RONDB_REST_SERVER_IP_DEFAULT);
      java.net.InetAddress inetAddress = java.net.InetAddress.getByName(grpcServerHostname);
      grpcServerIP = inetAddress.getHostAddress();
      grpcServerPort = Integer.parseInt(properties.getProperty(ConfigKeys.RONDB_GRPC_SERVER_PORT_KEY,
          Integer.toString(ConfigKeys.RONDB_GRPC_SERVER_PORT_DEFAULT)));
      String grpcServerAddress = grpcServerIP + ":" + grpcServerPort;
      logger.info("Connecting to gRPC test endpoint " + grpcServerAddress);

      channel = ManagedChannelBuilder.forAddress(grpcServerIP,
          grpcServerPort).usePlaintext().build();
      blockingStub = RonDBRESTGrpc.newBlockingStub(channel);

      test();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * This tests the REST client connection.
   */
  private void test() throws DBException {
    blockingStub.stat(RonDBGrpcProto.StatRequestProto.newBuilder().build());
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    PKReadRequestProto pkRead = createPKRequestProto(table, key, fields);
    RonDBGrpcProto.PKReadResponseProto response = blockingStub.pKRead(pkRead);
    if (response == null || !response.isInitialized()) {
      return Status.NOT_FOUND;
    }

    Map<String, ColumnValueProto> dataMap = response.getDataMap();
    for (Map.Entry<String, ColumnValueProto> entry : dataMap.entrySet()) {
      byte[] value = entry.getValue().toByteArray();
      result.put(entry.getKey(), new ByteArrayByteIterator(value, 0, value.length));
    }
    return Status.OK;
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          HashMap<String, HashMap<String, ByteIterator>> results) {
    RonDBGrpcProto.BatchRequestProto.Builder batchReqBuilder =
        RonDBGrpcProto.BatchRequestProto.newBuilder();
    batchReqBuilder.setAPIKey("Dummy Key");

    // create operations
    for (int i = 0; i < keys.size(); i++) {
      String pk = keys.get(i);
      Set<String> projectionFields = fields.get(i);
      batchReqBuilder.addOperations(createPKRequestProto(table, pk, projectionFields));
    }

    RonDBGrpcProto.BatchResponseProto response = blockingStub.batch(batchReqBuilder.build());
    if (response == null || !response.isInitialized()) {
      return Status.ERROR;
    }

    boolean allGood = true;
    // unpack the response
    for (int i = 0; i < response.getResponsesCount(); i++) {
      RonDBGrpcProto.PKReadResponseProto pkResponse = response.getResponses(i);
      if (pkResponse.getCode() != 200){
        allGood = false;
        break;
      }

      String pk =  pkResponse.getOperationID();
      HashMap<String, ByteIterator> result = results.get(pk);
      assert result != null;

      for (Map.Entry<String, ColumnValueProto> entry : pkResponse.getDataMap().entrySet()) {
        byte[] value = entry.getValue().toByteArray();
        result.put(entry.getKey(), new ByteArrayByteIterator(value, 0, value.length));
      }
    }

    if(allGood) {
      return Status.OK;
    } else {
      return Status.ERROR;
    }
  }

  private PKReadRequestProto createPKRequestProto(String table, String pk,
                                                  Set<String> fields) {
    PKReadRequestProto.Builder pkReadBuilder = PKReadRequestProto.newBuilder();
    pkReadBuilder.setDB(databaseName);
    pkReadBuilder.setTable(table);
    pkReadBuilder.setOperationID(pk/*operation id*/);
    pkReadBuilder.setAPIKey("Dummy Key");
    pkReadBuilder.addFilters(FilterProto.newBuilder().setColumn(UserTableHelper.KEY).setValue(pk).build());
    for (String field : fields) {
      pkReadBuilder.addReadColumns(ReadColumnProto.newBuilder().setColumn(field).build());
    }
    PKReadRequestProto pkRead = pkReadBuilder.build();
    for (String field : fields) {
      pkReadBuilder.addReadColumns(ReadColumnProto.newBuilder().setColumn(field).build());
    }
    return pkReadBuilder.build();
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
    channel.shutdown();
  }
}
