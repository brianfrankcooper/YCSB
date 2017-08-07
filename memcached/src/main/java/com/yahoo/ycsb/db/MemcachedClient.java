/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
// We also use `net.spy.memcached.MemcachedClient`; it is not imported
// explicitly and referred to with its full path to avoid conflicts with the
// class of the same name in this file.
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.apache.log4j.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Concrete Memcached client implementation.
 */
public class MemcachedClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private boolean checkOperationStatus;
  private long shutdownTimeoutMillis;
  private int objectExpirationTime;

  public static final String HOSTS_PROPERTY = "memcached.hosts";

  public static final int DEFAULT_PORT = 11211;

  private static final String TEMPORARY_FAILURE_MSG = "Temporary failure";
  private static final String CANCELLED_MSG = "cancelled";

  public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY =
      "memcached.shutdownTimeoutMillis";
  public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

  public static final String OBJECT_EXPIRATION_TIME_PROPERTY =
      "memcached.objectExpirationTime";
  public static final String DEFAULT_OBJECT_EXPIRATION_TIME =
      String.valueOf(Integer.MAX_VALUE);

  public static final String CHECK_OPERATION_STATUS_PROPERTY =
      "memcached.checkOperationStatus";
  public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

  public static final String READ_BUFFER_SIZE_PROPERTY =
      "memcached.readBufferSize";
  public static final String DEFAULT_READ_BUFFER_SIZE = "3000000";

  public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeoutMillis";
  public static final String DEFAULT_OP_TIMEOUT = "60000";

  public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";
  public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT =
      FailureMode.Redistribute;

  public static final String PROTOCOL_PROPERTY = "memcached.protocol";
  public static final ConnectionFactoryBuilder.Protocol DEFAULT_PROTOCOL =
      ConnectionFactoryBuilder.Protocol.TEXT;

  /**
   * The MemcachedClient implementation that will be used to communicate
   * with the memcached server.
   */
  private net.spy.memcached.MemcachedClient client;

  /**
   * @returns Underlying Memcached protocol client, implemented by
   *     SpyMemcached.
   */
  protected net.spy.memcached.MemcachedClient memcachedClient() {
    return client;
  }

  @Override
  public void init() throws DBException {
    try {
      client = createMemcachedClient();
      checkOperationStatus = Boolean.parseBoolean(
          getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY,
                                      CHECK_OPERATION_STATUS_DEFAULT));
      objectExpirationTime = Integer.parseInt(
          getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY,
                                      DEFAULT_OBJECT_EXPIRATION_TIME));
      shutdownTimeoutMillis = Integer.parseInt(
          getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY,
                                      DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  protected net.spy.memcached.MemcachedClient createMemcachedClient()
      throws Exception {
    ConnectionFactoryBuilder connectionFactoryBuilder =
        new ConnectionFactoryBuilder();

    connectionFactoryBuilder.setReadBufferSize(Integer.parseInt(
        getProperties().getProperty(READ_BUFFER_SIZE_PROPERTY,
                                    DEFAULT_READ_BUFFER_SIZE)));

    connectionFactoryBuilder.setOpTimeout(Integer.parseInt(
        getProperties().getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT)));

    String protocolString = getProperties().getProperty(PROTOCOL_PROPERTY);
    connectionFactoryBuilder.setProtocol(
        protocolString == null ? DEFAULT_PROTOCOL
                         : ConnectionFactoryBuilder.Protocol.valueOf(protocolString.toUpperCase()));

    String failureString = getProperties().getProperty(FAILURE_MODE_PROPERTY);
    connectionFactoryBuilder.setFailureMode(
        failureString == null ? FAILURE_MODE_PROPERTY_DEFAULT
                              : FailureMode.valueOf(failureString));

    // Note: this only works with IPv4 addresses due to its assumption of
    // ":" being the separator of hostname/IP and port; this is not the case
    // when dealing with IPv6 addresses.
    //
    // TODO(mbrukman): fix this.
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    String[] hosts = getProperties().getProperty(HOSTS_PROPERTY).split(",");
    for (String address : hosts) {
      int colon = address.indexOf(":");
      int port = DEFAULT_PORT;
      String host = address;
      if (colon != -1) {
        port = Integer.parseInt(address.substring(colon + 1));
        host = address.substring(0, colon);
      }
      addresses.add(new InetSocketAddress(host, port));
    }
    return new net.spy.memcached.MemcachedClient(
        connectionFactoryBuilder.build(), addresses);
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      GetFuture<Object> future = memcachedClient().asyncGet(key);
      Object document = future.get();
      if (document != null) {
        fromJson((String) document, fields, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future =
          memcachedClient().replace(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future =
          memcachedClient().add(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future = memcachedClient().delete(key);
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  protected Status getReturnCode(OperationFuture<Boolean> future) {
    if (!checkOperationStatus) {
      return Status.OK;
    }
    if (future.getStatus().isSuccess()) {
      return Status.OK;
    } else if (TEMPORARY_FAILURE_MSG.equals(future.getStatus().getMessage())) {
      return new Status("TEMPORARY_FAILURE", TEMPORARY_FAILURE_MSG);
    } else if (CANCELLED_MSG.equals(future.getStatus().getMessage())) {
      return new Status("CANCELLED_MSG", CANCELLED_MSG);
    }
    return new Status("ERROR", future.getStatus().getMessage());
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      memcachedClient().shutdown(shutdownTimeoutMillis, MILLISECONDS);
    }
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
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
