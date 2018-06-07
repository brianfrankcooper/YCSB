/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.db.arangodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.Protocol;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.TransactionOptions;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * ArangoDB binding for YCSB framework using the ArangoDB Inc. <a
 * href="https://github.com/arangodb/arangodb-java-driver">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @see <a href="https://github.com/arangodb/arangodb-java-driver">ArangoDB Inc.
 *      driver</a>
 */
public class ArangoDBClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(ArangoDBClient.class);
  
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** ArangoDB Driver related, Singleton. */
  private ArangoDB arangoDB;
  private String databaseName = "ycsb";
  private String collectionName;
  private Boolean dropDBBeforeRun;
  private Boolean waitForSync = false;
  private Boolean transactionUpdate = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   * 
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void init() throws DBException {
    synchronized (ArangoDBClient.class) {
      Properties props = getProperties();

      collectionName = props.getProperty("table", "usertable");

      // Set the DB address
      String ip = props.getProperty("arangodb.ip", "localhost");
      String portStr = props.getProperty("arangodb.port", "8529");
      int port = Integer.parseInt(portStr);

      // Set network protocol
      String protocolStr = props.getProperty("arangodb.protocol", "VST");
      Protocol protocol = Protocol.valueOf(protocolStr);

      // If clear db before run
      String dropDBBeforeRunStr = props.getProperty("arangodb.dropDBBeforeRun", "false");
      dropDBBeforeRun = Boolean.parseBoolean(dropDBBeforeRunStr);
      
      // Set the sync mode
      String waitForSyncStr = props.getProperty("arangodb.waitForSync", "false");
      waitForSync = Boolean.parseBoolean(waitForSyncStr);
      
      // Set if transaction for update
      String transactionUpdateStr = props.getProperty("arangodb.transactionUpdate", "false");
      transactionUpdate = Boolean.parseBoolean(transactionUpdateStr);
      
      // Init ArangoDB connection
      try {
        arangoDB = new ArangoDB.Builder().host(ip).port(port).useProtocol(protocol).build();
      } catch (Exception e) {
        logger.error("Failed to initialize ArangoDB", e);
        System.exit(-1);
      }

      if(INIT_COUNT.getAndIncrement() == 0) {
        // Init the database
        if (dropDBBeforeRun) {
          // Try delete first
          try {
            arangoDB.db(databaseName).drop();
          } catch (ArangoDBException e) {
            logger.info("Fail to delete DB: {}", databaseName);
          }
        }
        try {
          arangoDB.createDatabase(databaseName);
          logger.info("Database created: " + databaseName);
        } catch (ArangoDBException e) {
          logger.error("Failed to create database: {} with ex: {}", databaseName, e.toString());
        }
        try {
          arangoDB.db(databaseName).createCollection(collectionName);
          logger.info("Collection created: " + collectionName);
        } catch (ArangoDBException e) {
          logger.error("Failed to create collection: {} with ex: {}", collectionName, e.toString());
        }
        logger.info("ArangoDB client connection created to {}:{}", ip, port);

        // Log the configuration
        logger.info("Arango Configuration: dropDBBeforeRun: {}; address: {}:{}; databaseName: {};"
                    + " waitForSync: {}; transactionUpdate: {};",
                    dropDBBeforeRun, ip, port, databaseName, waitForSync, transactionUpdate);
      }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   * 
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      arangoDB.shutdown();
      arangoDB = null;
      logger.info("Local cleaned up.");
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to insert.
   * @param values
   *      A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      BaseDocument toInsert = new BaseDocument(key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
      }
      DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
      arangoDB.db(databaseName).collection(table).insertDocument(toInsert, options);
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying insert {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   * 
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to read.
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      VPackSlice document = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);
      if (!this.fillMap(result, document, fields)) {
        return Status.ERROR;
      }
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying read {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to write.
   * @param values
   *      A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *     description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (!transactionUpdate) {
        BaseDocument updateDoc = new BaseDocument();
        for (Entry<String, ByteIterator> field : values.entrySet()) {
          updateDoc.addAttribute(field.getKey(), byteIteratorToString(field.getValue()));
        }
        arangoDB.db(databaseName).collection(table).updateDocument(key, updateDoc);
        return Status.OK;
      } else {
        // id for documentHandle
        String transactionAction = "function (id) {"
               // use internal database functions
            + "var db = require('internal').db;"
              // collection.update(document, data, overwrite, keepNull, waitForSync)
            + String.format("db._update(id, %s, true, false, %s);}",
                mapToJson(values), Boolean.toString(waitForSync).toLowerCase());
        TransactionOptions options = new TransactionOptions();
        options.writeCollections(table);
        options.params(createDocumentHandle(table, key));
        arangoDB.db(databaseName).transaction(transactionAction, Void.class, options);
        return Status.OK;
      }
    } catch (ArangoDBException e) {
      logger.error("Exception while trying update {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      arangoDB.db(databaseName).collection(table).deleteDocument(key);
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying delete {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   * 
   * @param table
   *      The name of the table
   * @param startkey
   *      The record key of the first record to read.
   * @param recordcount
   *      The number of records to read
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A Vector of HashMaps, where each HashMap is a set field/value
   *      pairs for one record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    ArangoCursor<VPackSlice> cursor = null;
    try {
      String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
          recordcount, constructReturnForAQL(fields, "target"));

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
      cursor = arangoDB.db(databaseName).query(aqlQuery, bindVars, null, VPackSlice.class);
      while (cursor.hasNext()) {
        VPackSlice aDocument = cursor.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
        if (!this.fillMap(aMap, aDocument)) {
          return Status.ERROR;
        }
        result.add(aMap);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying scan {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    } finally {
      if (cursor != null) {
        try {
          cursor.close();
        } catch (IOException e) {
          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }

  private String createDocumentHandle(String collection, String documentKey) throws ArangoDBException {
    validateCollectionName(collection);
    return collection + "/" + documentKey;
  }

  private void validateCollectionName(String name) throws ArangoDBException {
    if (name.indexOf('/') != -1) {
      throw new ArangoDBException("does not allow '/' in name.");
    }
  }

  
  private String constructReturnForAQL(Set<String> fields, String targetName) {
    // Construct the AQL query string.
    String resultDes = targetName;
    if (fields != null && fields.size() != 0) {
      StringBuilder builder = new StringBuilder("{");
      for (String field : fields) {
        builder.append(String.format("\n\"%s\" : %s.%s,", field, targetName, field));
      }
      //Replace last ',' to newline.
      builder.setCharAt(builder.length() - 1, '\n');
      builder.append("}");
      resultDes = builder.toString();
    }
    return resultDes;
  }
  
  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document) {
    return fillMap(resultMap, document, null);
  }
  
  /**
   * Fills the map with the properties from the BaseDocument.
   * 
   * @param resultMap
   *      The map to fill/
   * @param document
   *      The record to read from
   * @param fields
   *      The list of fields to read, or null for all of them
   * @return isSuccess
   */
  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document, Set<String> fields) {
    if (fields == null || fields.size() == 0) {
      for (Iterator<Entry<String, VPackSlice>> iterator = document.objectIterator(); iterator.hasNext();) {
        Entry<String, VPackSlice> next = iterator.next();
        VPackSlice value = next.getValue();
        if (value.isString()) {
          resultMap.put(next.getKey(), stringToByteIterator(value.getAsString()));
        } else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    } else {
      for (String field : fields) {
        VPackSlice value = document.get(field);
        if (value.isString()) {
          resultMap.put(field, stringToByteIterator(value.getAsString()));
        } else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    }
    return true;
  }
  
  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  private ByteIterator stringToByteIterator(String content) {
    return new StringByteIterator(content);
  }
  
  private String mapToJson(Map<String, ByteIterator> values) {
    VPackBuilder builder = new VPackBuilder().add(ValueType.OBJECT);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      builder.add(entry.getKey(), byteIteratorToString(entry.getValue()));
    }
    builder.close();
    return arangoDB.util().deserialize(builder.slice(), String.class);
  }
  
}
