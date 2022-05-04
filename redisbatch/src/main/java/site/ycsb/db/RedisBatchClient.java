/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import java.util.ArrayList;
import java.util.Random;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisBatchClient extends DB {

  private Jedis primary;
  private Jedis[] replicas;

  public static final String PRIMARY_HOST_PROPERTY = "redis.primary-host";
  public static final String REPLICA_HOST_PROPERTY = "redis.replica-host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  private static long recordcount = 0;

  private static int batchsize = 1;

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String primaryHost = props.getProperty(PRIMARY_HOST_PROPERTY);
    String[] replicaHosts = props.getProperty(REPLICA_HOST_PROPERTY).split(",");

    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
    if (redisTimeout != null){
      primary = new Jedis(primaryHost, port, Integer.parseInt(redisTimeout));
    } else {
      primary = new Jedis(primaryHost, port);
    }
    primary.connect();

    replicas = new Jedis[replicaHosts.length];
    for (int index = 0; index < replicaHosts.length; index++) {
      String host = replicaHosts[index];
      Jedis replica;
      if (redisTimeout != null){
        replica = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        replica = new Jedis(host, port);
      }
      replica.connect();
      replicas[index] = replica;
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) primary).auth(password);
    }

    recordcount = Long.parseLong(getProperties().getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

    batchsize = Integer.parseInt(getProperties().getProperty("batchsize", "1"));
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) primary).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    List<String> keys = new ArrayList<String>();
    for (int i = 0; i < batchsize; i++) {
      keys.add("user" + String.valueOf(new Random().nextInt((int)recordcount)));
    }

    // pick a random replica to send read request to
    Jedis replica = replicas[new Random().nextInt(replicas.length)];
    Pipeline p = replica.pipelined();
    Map<String, Response<Map<String, String>>> allFieldsRes = new HashMap<>();
    Map<String, Response<List<String>>> partialFieldsRes = new HashMap<>();

    keys.forEach(k -> {
      if (fields == null) {
        allFieldsRes.put(k, p.hgetAll(k));
      } else {
        String[] fieldArray =
            (String[]) fields.toArray(new String[fields.size()]);
        partialFieldsRes.put(k, p.hmget(k, fieldArray));
      }
    });

    p.sync();

    keys.forEach(k -> {
      if (fields == null) {
        StringByteIterator.putAllAsByteIterators(result, allFieldsRes.get(k).get());
      } else {
        List<String> values = partialFieldsRes.get(k).get();

        Iterator<String> fieldIterator = fields.iterator();
        Iterator<String> valueIterator = values.iterator();

        while (fieldIterator.hasNext() && valueIterator.hasNext()) {
          result.put(fieldIterator.next(),
              new StringByteIterator(valueIterator.next()));
        }
        assert !fieldIterator.hasNext() && !valueIterator.hasNext();
      }
    });

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (primary.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      primary.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return primary.del(key) == 0 && primary.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return primary.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = primary.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
