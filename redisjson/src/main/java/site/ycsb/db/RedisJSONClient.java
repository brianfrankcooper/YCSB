/**
 * Copyright (c) 2020 Redis Labs. All rights reserved.
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
 * Redis JSON client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis JSON structure; table is ignored and
 * key is used as the primary index. All fields are located at the root path of the JSON
 * DOM and values are mapped thereafter as simple scalars due to JReJSON's inability to
 * instantiate proper POJOs from stored JSON objects.
 *
 * For scanning operations, all keys are saved a sorted set with an equal score allowing for lexigraphical retrieval.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redisjson.io/">RedisJSON</a>.
 *
 * See {@code redisjson/README.md} for details.
 */
public class RedisJSONClient extends DB {

    /* Blatant rip-off of the original Jedis-based driver class WITHOUT cluster support */

  private JReJSON jedisJSON;
  private Jedis jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
    //  public static final String CLUSTER_PROPERTY = "redis.cluster";

  public static final String INDEX_KEY = "_indices_JSON";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    /* 
       boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
    */
    jedisJSON = new JReJSON(host, port);
    jedis = new Jedis(host, port);
    jedis.connect();

    /*
    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
      }*/
  }

  public void cleanup() throws DBException {
    try {
      jedis.close();
    } catch (Exception e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    HashMap<String, String> map = new HashMap<String, String>();
    map = jedisJSON.get(key, map.getClass(), Path.ROOT_PATH);

    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, map);
    } else {
      for (String field : fields) {
        result.put(field, new StringByteIterator(map.get(field)));
      }
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    Status status = Status.OK;
    try {
      jedisJSON.set(key, StringByteIterator.getStringMap(values));
    } catch (Exception e) {
      status = Status.ERROR;
    }
    if (status == Status.OK) {
      jedis.zadd(INDEX_KEY, 1.0, key);
    }
    return status;
  }

  @Override
  public Status delete(String table, String key) {
    Status status = Status.OK;
    try {
      jedisJSON.del(key);
    } catch (Exception e) {
      status = Status.ERROR;
    }
    if (status == Status.OK) {
      jedis.zrem(INDEX_KEY, key);
    }
    return status;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Status status = Status.OK;
    try {
      HashMap<String, String> map = new HashMap<String, String>();
      map = jedisJSON.get(key, map.getClass(), Path.ROOT_PATH);
      // Merge changed fields
      map.putAll(StringByteIterator.getStringMap(values));
      jedisJSON.set(key, StringByteIterator.getStringMap(StringByteIterator.getByteIteratorMap(map)));
    } catch (Exception e) {
      status = Status.ERROR;
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Long index = jedis.zrank(INDEX_KEY, startkey);
    Set<String> keys = jedis.zrange(INDEX_KEY, index, index + recordcount - 1);

    for (String key : keys) {
      HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }
/**
   * Prefix the key with the given prefix, to establish a unique namespace.
   *
   * @param prefix the prefix to use.
   * @param key the actual key.
   * @return the formatted and prefixed key.

  private String formatKey(final String prefix, final String key) {
    return prefix + ":" + key;
  }
*/
}
