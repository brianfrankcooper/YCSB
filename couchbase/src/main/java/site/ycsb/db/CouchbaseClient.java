/**
 * Copyright (c) 2013 - 2016 YCSB contributors. All rights reserved.
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

import com.couchbase.client.protocol.views.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A class that wraps the CouchbaseClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class CouchbaseClient extends DB {
  public static final String URL_PROPERTY = "couchbase.url";
  public static final String BUCKET_PROPERTY = "couchbase.bucket";
  public static final String PASSWORD_PROPERTY = "couchbase.password";
  public static final String CHECKF_PROPERTY = "couchbase.checkFutures";
  public static final String PERSIST_PROPERTY = "couchbase.persistTo";
  public static final String REPLICATE_PROPERTY = "couchbase.replicateTo";
  public static final String JSON_PROPERTY = "couchbase.json";
  public static final String DESIGN_DOC_PROPERTY = "couchbase.ddoc";
  public static final String VIEW_PROPERTY = "couchbase.view";
  public static final String STALE_PROPERTY = "couchbase.stale";
  public static final String SCAN_PROPERTY = "scanproportion";

  public static final String STALE_PROPERTY_DEFAULT = Stale.OK.name();
  public static final String SCAN_PROPERTY_DEFAULT = "0.0";

  protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private com.couchbase.client.CouchbaseClient client;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private boolean checkFutures;
  private boolean useJson;
  private String designDoc;
  private String viewName;
  private Stale stale;
  private View view;
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

    designDoc = getProperties().getProperty(DESIGN_DOC_PROPERTY);
    viewName = getProperties().getProperty(VIEW_PROPERTY);
    stale = Stale.valueOf(getProperties().getProperty(STALE_PROPERTY, STALE_PROPERTY_DEFAULT).toUpperCase());

    Double scanproportion = Double.valueOf(props.getProperty(SCAN_PROPERTY, SCAN_PROPERTY_DEFAULT));

    Properties systemProperties = System.getProperties();
    systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SLF4JLogger");
    System.setProperties(systemProperties);

    try {
      client = new com.couchbase.client.CouchbaseClient(Arrays.asList(new URI(url)), bucket, password);
    } catch (Exception e) {
      throw new DBException("Could not create CouchbaseClient object.", e);
    }

    if (scanproportion > 0) {
      try {
        view = client.getView(designDoc, viewName);
      } catch (Exception e) {
        throw new DBException(String.format("%s=%s and %s=%s provided, unable to connect to view.",
          DESIGN_DOC_PROPERTY, designDoc, VIEW_PROPERTY, viewName), e.getCause());
      }
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
    case 0:
      return ReplicateTo.ZERO;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
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
    case 0:
      return PersistTo.ZERO;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
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
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    String formattedKey = formatKey(table, key);

    try {
      Object loaded = client.get(formattedKey);

      if (loaded == null) {
        return Status.ERROR;
      }

      decode(loaded, fields, result);
      return Status.OK;
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not read value for key " + formattedKey, e);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      Query query = new Query().setRangeStart(startkey)
          .setLimit(recordcount)
          .setIncludeDocs(true)
          .setStale(stale);
      ViewResponse response = client.query(view, query);

      for (ViewRow row : response) {
        HashMap<String, ByteIterator> rowMap = new HashMap();
        decode(row.getDocument(), fields, rowMap);
        result.add(rowMap);
      }

      return Status.OK;
    } catch (Exception e) {
      log.error(e.getMessage());
    }

    return Status.ERROR;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.replace(formattedKey, encode(values), persistTo, replicateTo);
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not update value for key " + formattedKey, e);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.add(formattedKey, encode(values), persistTo, replicateTo);
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not insert value for key " + formattedKey, e);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    String formattedKey = formatKey(table, key);

    try {
      final OperationFuture<Boolean> future = client.delete(formattedKey, persistTo, replicateTo);
      return checkFutureStatus(future);
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not delete value for key " + formattedKey, e);
      }
      return Status.ERROR;
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
  private Status checkFutureStatus(final OperationFuture<?> future) {
    if (checkFutures) {
      return future.getStatus().isSuccess() ? Status.OK : Status.ERROR;
    } else {
      return Status.OK;
    }
  }

  /**
   * Decode the object from server into the storable result.
   *
   * @param source the loaded object.
   * @param fields the fields to check.
   * @param dest the result passed back to the ycsb core.
   */
  private void decode(final Object source, final Set<String> fields, final Map<String, ByteIterator> dest) {
    if (useJson) {
      try {
        JsonNode json = JSON_MAPPER.readTree((String) source);
        boolean checkFields = fields != null && !fields.isEmpty();
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
      Map<String, String> converted = (HashMap<String, String>) source;
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
  private Object encode(final Map<String, ByteIterator> source) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(source);
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
