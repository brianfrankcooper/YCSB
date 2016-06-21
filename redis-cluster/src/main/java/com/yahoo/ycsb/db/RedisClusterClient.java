/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

/**
 * Redis Cluster client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*. For scanning operations, all keys are saved
 * (by an arbitrary hash) in a sorted set. This implementation mimics the current Redis client
 * implementation.
 */

package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import com.fabahaba.jedipus.cluster.CRC16;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.primitive.RedisClientFactory;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * YCSB binding for <a href="http://redis.io/topics/cluster-spec">Redis Cluster</a>.
 *
 * See {@code redis-cluster/README.md} for details.
 */
public class RedisClusterClient extends DB {

  private static final String HOST =
      Optional.ofNullable(System.getProperty("redis.cluster.host")).orElse("localhost");
  private static final int PORT = Optional.ofNullable(System.getProperty("redis.cluster.port"))
      .map(Integer::parseInt).orElse(7000);
  private static final String PASS =
      Optional.ofNullable(System.getProperty("redis.cluster.password")).orElse(null);
  private static final int MAX_POOL_SIZE =
      Optional.ofNullable(System.getProperty("redis.cluster.pool.maxclients"))
          .map(Integer::parseInt).orElse(Runtime.getRuntime().availableProcessors());

  private static final ClientPool.Builder POOL_BUILDER =
      ClientPool.startBuilding().withBlockWhenExhausted(true).withMaxTotal(MAX_POOL_SIZE);

  private static final RedisClientFactory.Builder CLIENT_FACTORY_BUILDER =
      RedisClientFactory.startBuilding().withAuth(PASS);

  private static final String INDEX_KEY = "_indices";
  private static final byte[] INDEX_KEY_BYTES = RESP.toBytes(INDEX_KEY);
  private static final int INDEX_SLOT = CRC16.getSlot(INDEX_KEY_BYTES);

  private static final RedisClusterExecutor RCE = RedisClusterExecutor
      .startBuilding(Node.create(HOST, PORT)).withReadMode(ReadMode.MIXED)
      .withMasterPoolFactory(node -> POOL_BUILDER.create(CLIENT_FACTORY_BUILDER.createPooled(node)))
      .withSlavePoolFactory(
          node -> POOL_BUILDER.create(CLIENT_FACTORY_BUILDER.createPooled(node, true)))
      .withNodeUnknownFactory(CLIENT_FACTORY_BUILDER::create).create();

  /*
   * Calculate a hash for a key to store it in an index. The actual return value of this function is
   * not interesting -- it primarily needs to be fast and scattered along the whole space of
   * doubles. In a real world scenario one would probably use the ASCII values of the keys.
   */
  private static byte[] hash(final String key) {
    return RESP.toBytes(key.hashCode());
  }

  @Override
  public void cleanup() throws DBException {
    RCE.close();
  }

  @Override
  public Status delete(final String table, final String key) {
    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = CRC16.getSlot(keyBytes);

    final long keyRemoved =
        RCE.applyPrim(ReadMode.MASTER, slot, client -> client.sendCmd(Cmds.DEL.prim(), keyBytes));
    if (keyRemoved == 0) {
      return Status.ERROR;
    }
    final long indexRemoval = RCE.applyPrim(ReadMode.MASTER, INDEX_SLOT,
        client -> client.sendCmd(Cmds.ZREM.prim(), INDEX_KEY_BYTES, keyBytes));
    return indexRemoval == 0 ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = CRC16.getSlot(keyBytes);

    final byte[][] args = createKeyFieldValueArgs(keyBytes, values);
    final String response =
        RCE.apply(ReadMode.MASTER, slot, client -> client.sendCmd(Cmds.HMSET, args));

    if (response.equals(RESP.OK)) {
      final byte[][] zaddArgs = new byte[][] {INDEX_KEY_BYTES, hash(key), keyBytes};
      RCE.accept(ReadMode.MASTER, INDEX_SLOT, client -> client.sendCmd(Cmds.ZADD.raw(), zaddArgs));
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = CRC16.getSlot(keyBytes);

    if (fields == null) {
      final Object[] fieldValues =
          RCE.apply(slot, client -> client.sendCmd(Cmds.HGETALL, keyBytes));
      for (int i = 0; i < fieldValues.length;) {
        result.put((String) fieldValues[i++], new StringByteIterator((String) fieldValues[i++]));
      }
      return result.isEmpty() ? Status.ERROR : Status.OK;
    }

    final String[] orderedFields = new String[fields.size()];
    final byte[][] fieldBytesArray = new byte[orderedFields.length + 1][];
    fieldBytesArray[0] = keyBytes;
    int fieldIndex = 1;

    for (final String field : fields) {
      orderedFields[fieldIndex] = field;
      fieldBytesArray[fieldIndex++] = RESP.toBytes(field);
    }

    final Object[] values = RCE.apply(slot, client -> client.sendCmd(Cmds.HMGET, fieldBytesArray));
    fieldIndex = 0;
    for (final Object value : values) {
      result.put(orderedFields[fieldIndex++], new StringByteIterator((String) value));
    }
    return fieldIndex == orderedFields.length ? Status.OK : Status.ERROR;
  }

  private final CmdByteArray.Builder<Object[]> cmdArrayBuilder =
      CmdByteArray.startBuilding(Cmds.ZRANGEBYSCORE, 6);
  private static final byte[] POS_INF_BYTES = RESP.toBytes("+inf");
  private static final byte[] ZERO = RESP.toBytes(0);

  @Override
  public Status scan(final String table, final String startkey, final int recordcount,
      final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    final CmdByteArray<Object[]> cmdByteArray = cmdArrayBuilder.reset()
        .addArgs(INDEX_KEY_BYTES, hash(startkey), POS_INF_BYTES, ZERO, RESP.toBytes(recordcount))
        .create();
    final Object[] keys = RCE.apply(INDEX_SLOT, client -> client.sendDirect(cmdByteArray));

    final List<CompletableFuture<?>> readFutures = new ArrayList<>(keys.length);
    for (final Object key : keys) {
      final HashMap<String, ByteIterator> values = new HashMap<>();
      readFutures
          .add(CompletableFuture.runAsync(() -> read(table, RESP.toString(key), fields, values)));
      result.add(values);
    }
    readFutures.forEach(CompletableFuture::join);
    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = CRC16.getSlot(keyBytes);
    final byte[][] args = createKeyFieldValueArgs(keyBytes, values);
    final String response =
        RCE.apply(ReadMode.MASTER, slot, client -> client.sendCmd(Cmds.HMSET, args));
    return response.equals(RESP.OK) ? Status.OK : Status.ERROR;
  }

  private static byte[][] createKeyFieldValueArgs(final byte[] keyBytes,
      final HashMap<String, ByteIterator> values) {
    final byte[][] args = new byte[1 + 2 * values.size()][];
    args[0] = keyBytes;
    int index = 1;
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      args[index++] = RESP.toBytes(entry.getKey());
      args[index++] = entry.getValue().toArray();
    }
    return args;
  }
}
