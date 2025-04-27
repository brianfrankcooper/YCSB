/**
 * Copyright (c) 2023 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * etcd client binding for YCSB.
 * <p>
 */

package site.ycsb.db.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.time.Duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;


/**
 * YCSB binding for <a href="https://etcd.io">etcd</a>.
 *
 * See {@code etcd/README.md} for details.
 */
public class EtcdClient extends DB {
  private Client client;
  private KV kvClient;

  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String ENDPOINTS = "etcd.endpoints";
  private static final String DEFAULT_ENDPOINTS = "http://127.0.0.1:2379";
  private static final String SESSION_TIMEOUT_PROPERTY = "etcd.sessionTimeout";
  private static final long DEFAULT_SESSION_TIMEOUT = TimeUnit.SECONDS.toMillis(30L);

  public void init() throws DBException {
    Properties props = getProperties();

    String connectString = props.getProperty(ENDPOINTS);
    if (connectString == null || connectString.length() == 0) {
      connectString = DEFAULT_ENDPOINTS;
    }

    long sessionTimeout;
    String sessionTimeoutString = props.getProperty(SESSION_TIMEOUT_PROPERTY);
    if (sessionTimeoutString != null) {
      sessionTimeout = Integer.parseInt(sessionTimeoutString);
    } else {
      sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    }

    client = Client.builder()
        .connectTimeout(Duration.ofMillis(sessionTimeout))
        .endpoints(connectString.split(","))
        .build();
    kvClient = client.getKVClient();
  }

  @Override
  public void cleanup() {
    kvClient.close();
    client.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      ByteSequence keyByteSequence = ByteSequence.from(key, StandardCharsets.UTF_8);
      GetResponse getResponse = kvClient.get(keyByteSequence).get();

      if (getResponse.getKvs().isEmpty()) {
        return Status.NOT_FOUND;
      }

      ByteSequence value = getResponse.getKvs().get(0).getValue();
      deserializeValues(value.getBytes(), fields, result);
      return Status.OK;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> results) {
    try {
      ByteSequence startkeyByteSequence = ByteSequence.from(startkey, StandardCharsets.UTF_8);
      ByteSequence endkeyByteSequence = ByteSequence.from(new byte[] {0});
      GetOption getOption = GetOption.newBuilder()
          .withRange(endkeyByteSequence)
          .withLimit(recordcount)
          .build();
      GetResponse getResponse = kvClient.get(startkeyByteSequence, getOption).get();

      if (getResponse.getKvs().isEmpty()) {
        return Status.NOT_FOUND;
      }

      List<KeyValue> kvs = getResponse.getKvs();
      for (KeyValue kv : kvs) {
        ByteSequence value = kv.getValue();
        Map<String, ByteIterator> result = new HashMap<>();
        deserializeValues(value.getBytes(), fields, result);
        results.add(new HashMap<>(result));
      }

      return Status.OK;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      ByteSequence keyByteSequence = ByteSequence.from(key, StandardCharsets.UTF_8);
      String data = getJsonStrFromByteMap(values);
      ByteSequence valueByteSequence = ByteSequence.from(data, StandardCharsets.UTF_8);
      PutResponse putResponse = kvClient.put(keyByteSequence, valueByteSequence).get();
      return Status.OK;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      ByteSequence keyByteSequence = ByteSequence.from(key, StandardCharsets.UTF_8);
      kvClient.delete(keyByteSequence).get();
      return Status.OK;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] data, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    JSONObject jsonObject = (JSONObject) JSONValue.parse(new String(data, UTF_8));
    Iterator<String> iterator = jsonObject.keySet().iterator();
    while(iterator.hasNext()) {
      String field = iterator.next();
      String value = jsonObject.get(field).toString();
      if(fields == null || fields.contains(field)) {
        result.put(field, new StringByteIterator(value));
      }
    }
    return result;
  }

  private static String getJsonStrFromByteMap(Map<String, ByteIterator> map) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(map);
    return JSONValue.toJSONString(stringMap);
  }
}
