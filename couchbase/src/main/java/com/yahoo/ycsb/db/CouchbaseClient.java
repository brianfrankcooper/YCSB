/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.*;

/**
 * A class that wraps the CouchbaseClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>couchbase.url=http://127.0.0.1:8091/pools</b> The connection URL from one server.</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use./li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.checkFutures=true</b> If the futures should be inspected (makes ops sync).</li>
 * <li><b>couchbase.persistTo=0</b> Observe Persistence ("PersistTo" constraint)</li>
 * <li><b>couchbase.replicateTo=0</b> Observe Replication ("ReplicateTo" constraint)</li>
 * <li><b>couchbase.json=true</b> Use json or java serialization as target format.</li>
 * </ul>
 *
 * @author Michael Nitschinger
 */
public class CouchbaseClient extends DB {

  public static final String URL_PROPERTY = "couchbase.url";
  public static final String BUCKET_PROPERTY = "couchbase.bucket";
  public static final String PASSWORD_PROPERTY = "couchbase.password";
  public static final String CHECKF_PROPERTY = "couchbase.checkFutures";
  public static final String PERSIST_PROPERTY = "couchbase.persistTo";
  public static final String REPLICATE_PROPERTY = "couchbase.replicateTo";
  public static final String JSON_PROPERTY = "couchbase.json";

  public static final int OK = 0;
  public static final int FAILURE = 1;

  protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private com.couchbase.client.CouchbaseClient client;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private boolean checkFutures;
  private boolean useJson;
  private final Logger log = LoggerFactory.getLogger(getClass());

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String url = props.getProperty(URL_PROPERTY, "http://127.0.0.1:8091/pools");
    String bucket = props.getProperty(BUCKET_PROPERTY, "default");
    String password = props.getProperty(PASSWORD_PROPERTY, "");

    checkFutures = props.getProperty(CHECKF_PROPERTY, "true").equals("true");
    useJson = props.getProperty(JSON_PROPERTY, "true").equals("true");

    persistTo = parsePersistTo(props.getProperty(PERSIST_PROPERTY, "0"));
    replicateTo = parseReplicateTo(props.getProperty(REPLICATE_PROPERTY, "0"));

    Properties systemProperties = System.getProperties();
    systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
    System.setProperties(systemProperties);

    try {
      client = new com.couchbase.client.CouchbaseClient(
        Arrays.asList(new URI(url)),
        bucket,
        password
      );
    } catch (Exception e) {
      throw new DBException("Could not create CouchbaseClient object.", e);
    }
  }

  /**
   * Parse the replicate property into the correct enum.
   *
   * @param property the stringified property value.
   * @throws DBException if parsing the property did fail.
   * @return the correct enum.
   */
  private ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);

    switch (value) {
      case 0: return ReplicateTo.ZERO;
      case 1: return ReplicateTo.ONE;
      case 2: return ReplicateTo.TWO;
      case 3: return ReplicateTo.THREE;
      default:
        throw new DBException(REPLICATE_PROPERTY + " must be between 0 and 3");
    }
  }

  /**
   * Parse the persist property into the correct enum.
   *
   * @param property the stringified property value.
   * @throws DBException if parsing the property did fail.
   * @return the correct enum.
   */
  private PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);

    switch (value) {
      case 0: return PersistTo.ZERO;
      case 1: return PersistTo.ONE;
      case 2: return PersistTo.TWO;
      case 3: return PersistTo.THREE;
      case 4: return PersistTo.FOUR;
      default:
        throw new DBException(PERSIST_PROPERTY + " must be between 0 and 4");
    }
  }

  /**
   * Shutdown the client.
   */
  @Override
  public void cleanup() {
    client.shutdown();
  }

  @Override
  public int read(final String table, final String key, final Set<String> fields,
    final HashMap<String, ByteIterator> result) {
    String formattedKey = formatKey(table, key);

    try {
      Object loaded = client.get(formattedKey);

      if (loaded == null) {
        return FAILURE;
      }

      decode(loaded, fields, result);
      return OK;
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not read value for key " + formattedKey, e);
      }
      return FAILURE;
    }
  }

  /**
   * Scan is currently not implemented.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return FAILURE, because not implemented yet.
   */
  @Override
  public int scan(final String table, final String startkey, final int recordcount,
    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    return FAILURE;
  }

  @Override
  public int update(final String table, final String key, final HashMap<String, ByteIterator> values) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.replace(
        formattedKey,
        encode(values),
        persistTo,
        replicateTo
      );
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not update value for key " + formattedKey, e);
      }
      return FAILURE;
    }
  }

  @Override
  public int insert(final String table, final String key, final HashMap<String, ByteIterator> values) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.add(
        formattedKey,
        encode(values),
        persistTo,
        replicateTo
      );
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not insert value for key " + formattedKey, e);
      }
      return FAILURE;
    }
  }

  @Override
  public int delete(final String table, final String key) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.delete(formattedKey, persistTo, replicateTo);
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not delete value for key " + formattedKey, e);
      }
      return FAILURE;
    }
  }

  /**
   * Prefix the key with the given prefix, to establish a unique namespace.
   *
   * @param prefix the prefix to use.
   * @param key the actual key.
   * @return the formatted and prefixed key.
   */
  private String formatKey(final String prefix, final String key) {
    return prefix + ":" + key;
  }

  /**
   * Wrapper method that either inspects the future or not.
   *
   * @param future the future to potentially verify.
   * @return the status of the future result.
   */
  private int checkFutureStatus(final OperationFuture<?> future) {
    if (checkFutures) {
      return future.getStatus().isSuccess() ? OK : FAILURE;
    } else {
      return OK;
    }
  }

  /**
   * Decode the object from server into the storable result.
   *
   * @param source the loaded object.
   * @param fields the fields to check.
   * @param dest the result passed back to the ycsb core.
   */
  private void decode(final Object source, final Set<String> fields,
    final HashMap<String, ByteIterator> dest) {
    if (useJson) {
      try {
        JsonNode json = JSON_MAPPER.readTree((String) source);
        boolean checkFields = fields != null && fields.size() > 0;
        for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
          Map.Entry<String, JsonNode> jsonField = jsonFields.next();
          String name = jsonField.getKey();
          if (checkFields && fields.contains(name)) {
            continue;
          }
          JsonNode jsonValue = jsonField.getValue();
          if (jsonValue != null && !jsonValue.isNull()) {
            dest.put(name, new StringByteIterator(jsonValue.asText()));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Could not decode JSON");
      }
    } else {
      HashMap<String, String> converted = (HashMap<String, String>) source;
      for (Map.Entry<String, String> entry : converted.entrySet()) {
        dest.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    }
  }

  /**
   * Encode the object for couchbase storage.
   *
   * @param source the source value.
   * @return the storable object.
   */
  private Object encode(final HashMap<String, ByteIterator> source) {
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(source);
    if (!useJson) {
      return stringMap;
    }

    ObjectNode node = JSON_MAPPER.createObjectNode();
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
      JSON_MAPPER.writeTree(jsonGenerator, node);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode JSON value");
    }
    return writer.toString();
  }


}
