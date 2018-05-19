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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
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
public class RedisClient extends DB {

  private Jedis jedis;
  private boolean useStrings;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String USE_STRINGS_PROPERTY = "redis.usestrings";
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  public static final boolean USE_STRINGS_DEFAULT = false;

  public static final String INDEX_KEY = "_indices";

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

    jedis = new Jedis(host, port);
    jedis.connect();

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      jedis.auth(password);
    }

    String useStringsString = props.getProperty(USE_STRINGS_PROPERTY);
    if (useStringsString != null) {
      useStrings = Boolean.parseBoolean(useStringsString);
    } else {
      useStrings = USE_STRINGS_DEFAULT;
    }
  }

  public void cleanup() throws DBException {
    jedis.disconnect();
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

  public Status deleteUseStrings(String table, String key) {
    return jedis.del(key) == 0 ? Status.ERROR : Status.OK;
  }

  public Status readUseStrings(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    String value = jedis.get(key);
    if (value == null || value.isEmpty()) {
      return Status.ERROR;
    }
    try {
      fromJson(value, fields, result);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }

  }

  public Status insertUseStrings(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (jedis.set(key, toJson(values)).equals("OK")) {
        return Status.OK;
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.ERROR;
  }

  public Status scanUseStrings(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  public Status updateUseStrings(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (jedis.set(key, toJson(values)).equals("OK")) {
        return Status.OK;
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.ERROR;
  }


  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (useStrings) {
      return readUseStrings(table, key, fields, result);
    }
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (useStrings) {
      return insertUseStrings(table, key, values);
    }
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (useStrings) {
      return deleteUseStrings(table, key);
    }
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    if (useStrings) {
      return updateUseStrings(table, key, values);
    }
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (useStrings) {
      return scanUseStrings(table, startkey, recordcount, fields, result);
    }
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

  protected static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result)
      throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext();
    /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }

}
