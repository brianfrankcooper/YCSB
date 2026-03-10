/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
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

package site.ycsb.db;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.kvraft.proto.KVRequest;
import site.ycsb.db.kvraft.proto.KVResponse;
import site.ycsb.db.kvraft.proto.KVServiceGrpc;

/**
 * YCSB binding for kvraft.
 *
 * <p>Each YCSB record is stored as a single JSON document under one kvraft key.
 * Partial-field updates therefore perform a read-modify-write cycle at the client.
 */
public class KvraftClient extends DB {
  public static final String TARGET_PROPERTY = "kvraft.target";
  public static final String TABLE_PREFIX_PROPERTY = "kvraft.tableprefix";
  public static final String RPC_TIMEOUT_MS_PROPERTY = "kvraft.rpc_timeout_ms";
  public static final String MAX_REDIRECTS_PROPERTY = "kvraft.max_redirects";
  public static final String RPC_TIMEOUT_MS_DEFAULT = "3000";
  public static final String MAX_REDIRECTS_DEFAULT = "3";

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE =
      new TypeToken<Map<String, String>>() { }
          .getType();

  private ManagedChannel channel;
  private KVServiceGrpc.KVServiceBlockingStub client;
  private String currentTarget;
  private long rpcTimeoutMs;
  private int maxRedirects;
  private String tablePrefix;

  @Override
  public void init() throws DBException {
    final String target = getProperties().getProperty(TARGET_PROPERTY);
    if (target == null || target.trim().isEmpty()) {
      throw new DBException("Missing required property: " + TARGET_PROPERTY);
    }

    try {
      rpcTimeoutMs = Long.parseLong(
          getProperties().getProperty(RPC_TIMEOUT_MS_PROPERTY, RPC_TIMEOUT_MS_DEFAULT));
    } catch (NumberFormatException e) {
      throw new DBException("Invalid RPC timeout", e);
    }

    if (rpcTimeoutMs <= 0) {
      throw new DBException("RPC timeout must be greater than zero");
    }
    try {
      maxRedirects = Integer.parseInt(
          getProperties().getProperty(MAX_REDIRECTS_PROPERTY, MAX_REDIRECTS_DEFAULT));
    } catch (NumberFormatException e) {
      throw new DBException("Invalid redirect count", e);
    }
    if (maxRedirects < 0) {
      throw new DBException("Redirect count must be zero or greater");
    }

    tablePrefix = getProperties().getProperty(TABLE_PREFIX_PROPERTY, "");
    switchTarget(target.trim());
  }

  @Override
  public void cleanup() throws DBException {
    if (channel == null) {
      return;
    }

    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
      throw new DBException("Interrupted while closing kvraft gRPC channel", e);
    } finally {
      channel = null;
      client = null;
      currentTarget = null;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    DocumentResult current = fetchDocument(qualifiedKey(table, key));
    if (current.status != Status.OK) {
      return current.status;
    }

    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, current.document);
      return Status.OK;
    }

    for (String field : fields) {
      String value = current.document.get(field);
      if (value != null) {
        result.put(field, new StringByteIterator(value));
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    DocumentResult current = fetchDocument(qualifiedKey(table, key));
    if (current.status != Status.OK) {
      return current.status;
    }

    Map<String, String> updated = new HashMap<String, String>(current.document);
    StringByteIterator.putAllAsStrings(updated, values);
    return writeDocument(qualifiedKey(table, key), updated);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> document = new HashMap<String, String>();
    StringByteIterator.putAllAsStrings(document, values);
    return writeDocument(qualifiedKey(table, key), document);
  }

  @Override
  public Status delete(String table, String key) {
    ResponseResult result = invokeWithRedirects(new KvCall() {
      @Override
      public KVResponse invoke(KVServiceGrpc.KVServiceBlockingStub stub) {
        return stub.delete(KVRequest.newBuilder()
            .setKey(qualifiedKey(table, key))
            .build());
      }
    });
    return result.status;
  }

  private KVServiceGrpc.KVServiceBlockingStub stub() {
    return client.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS);
  }

  private void switchTarget(String target) throws DBException {
    if (target == null || target.trim().isEmpty()) {
      throw new DBException("kvraft target must not be empty");
    }
    final String normalizedTarget = target.trim();
    if (normalizedTarget.equals(currentTarget) && channel != null && client != null) {
      return;
    }

    ManagedChannel newChannel;
    try {
      newChannel = ManagedChannelBuilder.forTarget(normalizedTarget).usePlaintext().build();
    } catch (RuntimeException e) {
      throw new DBException("Failed to create kvraft channel for " + normalizedTarget, e);
    }

    ManagedChannel oldChannel = channel;
    channel = newChannel;
    client = KVServiceGrpc.newBlockingStub(newChannel);
    currentTarget = normalizedTarget;

    if (oldChannel != null) {
      oldChannel.shutdownNow();
    }
  }

  private String qualifiedKey(String table, String key) {
    if (tablePrefix != null && !tablePrefix.isEmpty()) {
      return tablePrefix + key;
    }
    return table + ":" + key;
  }

  private DocumentResult fetchDocument(String qualifiedKey) {
    try {
      ResponseResult result = invokeWithRedirects(new KvCall() {
        @Override
        public KVResponse invoke(KVServiceGrpc.KVServiceBlockingStub stub) {
          return stub.get(KVRequest.newBuilder().setKey(qualifiedKey).build());
        }
      });
      if (result.status != Status.OK) {
        return new DocumentResult(result.status, null);
      }
      return new DocumentResult(Status.OK, decodeDocument(result.response.getValue()));
    } catch (JsonSyntaxException e) {
      return new DocumentResult(Status.ERROR, null);
    }
  }

  private Status writeDocument(String qualifiedKey, Map<String, String> document) {
    ResponseResult result = invokeWithRedirects(new KvCall() {
      @Override
      public KVResponse invoke(KVServiceGrpc.KVServiceBlockingStub stub) {
        return stub.put(KVRequest.newBuilder()
            .setKey(qualifiedKey)
            .setValue(GSON.toJson(document))
            .build());
      }
    });
    return result.status;
  }

  private Map<String, String> decodeDocument(String encoded) {
    if (encoded == null || encoded.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> parsed = GSON.fromJson(encoded, STRING_MAP_TYPE);
    if (parsed == null) {
      return Collections.emptyMap();
    }
    return parsed;
  }

  private Status mapResponseError(String error) {
    final String normalized = error == null ? "" : error.trim().toLowerCase();
    if (normalized.contains("key not found")) {
      return Status.NOT_FOUND;
    }
    if (normalized.contains("not leader")) {
      return Status.SERVICE_UNAVAILABLE;
    }
    if (normalized.contains("timeout") || normalized.contains("server closed")) {
      return Status.SERVICE_UNAVAILABLE;
    }
    if (normalized.contains("invalid") || normalized.contains("bad request")) {
      return Status.BAD_REQUEST;
    }
    return Status.ERROR;
  }

  private boolean isNotLeader(KVResponse response) {
    return response != null && mapResponseError(response.getError()) == Status.SERVICE_UNAVAILABLE
        && response.getError() != null
        && response.getError().trim().toLowerCase().contains("not leader");
  }

  private ResponseResult invokeWithRedirects(KvCall call) {
    for (int attempt = 0; attempt <= maxRedirects; attempt++) {
      try {
        KVResponse response = call.invoke(stub());
        if (response.getSuccess()) {
          return new ResponseResult(Status.OK, response);
        }

        if (attempt < maxRedirects && isNotLeader(response)) {
          String leader = response.getLeader() == null ? "" : response.getLeader().trim();
          if (!leader.isEmpty() && !leader.equals(currentTarget)) {
            try {
              switchTarget(leader);
              continue;
            } catch (DBException e) {
              return new ResponseResult(Status.SERVICE_UNAVAILABLE, null);
            }
          }
        }

        return new ResponseResult(mapResponseError(response.getError()), response);
      } catch (StatusRuntimeException e) {
        return new ResponseResult(Status.SERVICE_UNAVAILABLE, null);
      }
    }
    return new ResponseResult(Status.SERVICE_UNAVAILABLE, null);
  }

  private static final class DocumentResult {
    private final Status status;
    private final Map<String, String> document;

    private DocumentResult(Status status, Map<String, String> document) {
      this.status = status;
      this.document = document;
    }
  }

  private static final class ResponseResult {
    private final Status status;
    private final KVResponse response;

    private ResponseResult(Status status, KVResponse response) {
      this.status = status;
      this.response = response;
    }
  }

  private interface KvCall {
    KVResponse invoke(KVServiceGrpc.KVServiceBlockingStub stub);
  }
}
