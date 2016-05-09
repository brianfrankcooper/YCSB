/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*. For scanning operations, all keys are saved
 * (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.primitive.JedisFactory;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.Protocol.Command;
import redis.clients.util.JedisClusterCRC16;

/**
 * YCSB binding for <a href="http://redis.io/topics/cluster-spec">Redis Cluster</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClusterClient extends DB {

  private JedisClusterExecutor jce;

  public static final String HOST_PROP = "redis.cluster.host";
  public static final String PORT_PROP = "redis.cluster.port";
  public static final String PASSWORD_PROP = "redis.cluster.password";

  public static final String MAX_POOL_SIZE_PROP = "redis.cluster.pool.maxclients";

  public static final String INDEX_KEY = "_indices";
  private static final byte[] INDEX_KEY_BYTES = RESP.toBytes("_indices");
  private static final int INDEX_SLOT = JedisClusterCRC16.getSlot(INDEX_KEY_BYTES);

  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    int port;
    final String portString = props.getProperty(PORT_PROP);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = 7000;
    }

    String host = props.getProperty(HOST_PROP);
    if (host == null) {
      host = Protocol.DEFAULT_HOST;
    }

    final GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
    poolConfig.setBlockWhenExhausted(true);

    final String maxPoolSize = props.getProperty(MAX_POOL_SIZE_PROP);
    if (maxPoolSize != null) {
      poolConfig.setMaxTotal(Integer.parseInt(maxPoolSize));
    } else {
      poolConfig.setMaxTotal(Runtime.getRuntime().availableProcessors());
    }

    final JedisFactory.Builder jedisFactory = JedisFactory.startBuilding();
    final String password = props.getProperty(PASSWORD_PROP);
    if (password != null) {
      jedisFactory.withAuth(password);
    }

    final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory =
        node -> new GenericObjectPool<>(jedisFactory.createPooled(node), poolConfig);

    final boolean readOnly = true;
    final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory =
        node -> new GenericObjectPool<>(jedisFactory.createPooled(node, readOnly), poolConfig);

    jce = JedisClusterExecutor.startBuilding(ClusterNode.create(host, port))
        .withReadMode(ReadMode.MIXED).withMasterPoolFactory(masterPoolFactory)
        .withSlavePoolFactory(slavePoolFactory).withNodeUnknownFactory(jedisFactory::create)
        .create();
  }

  @Override
  public void cleanup() throws DBException {
    jce.close();
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value of this function is
   * not interesting -- it primarily needs to be fast and scattered along the whole space of
   * doubles. In a real world scenario one would probably use the ASCII values of the keys.
   */
  private static double hash(final String key) {
    return key.hashCode();
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final HashMap<String, ByteIterator> result) {

    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = JedisClusterCRC16.getSlot(keyBytes);

    if (fields == null) {

      jce.applyJedis(slot, jedis -> jedis.hgetAll(keyBytes)).forEach(
          (field, value) -> result.put(RESP.toString(field), new ByteArrayByteIterator(value)));
      return result.isEmpty() ? Status.ERROR : Status.OK;
    }

    final String[] orderedFields = new String[fields.size()];
    final byte[][] fieldBytesArray = new byte[orderedFields.length][];
    int fieldIndex = 0;

    for (final String field : fields) {
      orderedFields[fieldIndex] = field;
      fieldBytesArray[fieldIndex++] = RESP.toBytes(field);
    }

    final List<byte[]> values =
        jce.applyJedis(slot, jedis -> jedis.hmget(keyBytes, fieldBytesArray));

    fieldIndex = 0;
    for (final byte[] value : values) {
      result.put(orderedFields[fieldIndex++], new ByteArrayByteIterator(value));
    }

    return fieldIndex == orderedFields.length ? Status.OK : Status.ERROR;
  }

  @Override
  public Status insert(final String table, final String key,
      final HashMap<String, ByteIterator> values) {

    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = JedisClusterCRC16.getSlot(keyBytes);

    final byte[][] args = createKeyFieldValueArgs(keyBytes, values);

    final String response = jce.applyJedis(ReadMode.MASTER, slot,
        jedis -> jedis.cmdWithStatusCodeReply(Command.HMSET, args));

    if (response.equals("OK")) {
      final byte[][] zaddArgs = new byte[][] {INDEX_KEY_BYTES, RESP.toBytes(hash(key)), keyBytes};
      jce.acceptJedis(ReadMode.MASTER, INDEX_SLOT,
          jedis -> jedis.cmdWithIntegerReply(Command.ZADD, zaddArgs));
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(final String table, final String key) {

    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = JedisClusterCRC16.getSlot(keyBytes);

    final long keyRemoved = jce.applyJedis(slot, jedis -> jedis.del(keyBytes));
    if (keyRemoved == 0) {
      return Status.ERROR;
    }

    final long indexRemoval = jce.applyJedis(ReadMode.MASTER, INDEX_SLOT,
        jedis -> jedis.cmdWithIntegerReply(Command.ZREM, INDEX_KEY_BYTES, keyBytes));

    if (indexRemoval == 0) {
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key,
      final HashMap<String, ByteIterator> values) {

    final byte[] keyBytes = RESP.toBytes(key);
    final int slot = JedisClusterCRC16.getSlot(keyBytes);

    final byte[][] args = createKeyFieldValueArgs(keyBytes, values);

    final String response = jce.applyJedis(ReadMode.MASTER, slot,
        jedis -> jedis.cmdWithStatusCodeReply(Command.HMSET, args));

    return response.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount,
      final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {

    final Set<String> keys = jce.applyJedis(INDEX_SLOT, jedis -> jedis.zrangeByScore(INDEX_KEY,
        hash(startkey), Double.POSITIVE_INFINITY, 0, recordcount));

    final List<Future<?>> readFutures = new ArrayList<>(keys.size());

    for (final String key : keys) {
      final HashMap<String, ByteIterator> values = new HashMap<>();
      // read(table, key, fields, values);
      readFutures.add(ForkJoinPool.commonPool().submit(() -> read(table, key, fields, values)));
      result.add(values);
    }

    try {
      for (final Future<?> readFuture : readFutures) {
        readFuture.get();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (final ExecutionException e) {
      return Status.ERROR;
    }

    return Status.OK;
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
