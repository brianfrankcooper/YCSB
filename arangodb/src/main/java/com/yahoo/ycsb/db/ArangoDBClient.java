/*
 * Arango client binding for YCSB.
 *
 * By Yemin, Sui
 */
package com.yahoo.ycsb.db;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.ArangoHost;
import com.arangodb.DocumentCursor;
import com.arangodb.ErrorNums;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionOptions;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.entity.EntityFactory;
import com.arangodb.entity.TransactionEntity;
import com.arangodb.util.MapBuilder;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This's thread-safe.
 * Assume the table is the same during whole procedure.
 * The data will be transfered from ByteIterator into String before sent to DB.
 * 
 * @author ysui
 */
public class ArangoDBClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(ArangoDBClient.class);
  
  /**
   * The database name to access.
   */
  private static String databaseName = "ycsb";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** ArangoDB Driver related, Singleton. */
  private static ArangoConfigure arangoConfigure;
  private static ArangoDriver arangoDriver;
  private static Boolean dropDBBeforeRun;
  private static Boolean waitForSync = false;
  private static Boolean transactionUpdate = false;
  // For creating collection manually with waitForSync param.
  private static String defaultCollection = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   * 
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (ArangoDBClient.class) {
      if (arangoDriver != null || arangoConfigure != null) {
        return;
      }

      Properties props = getProperties();

      // Set the DB address
      String ip = props.getProperty("arangodb.ip", "localhost");
      String portStr = props.getProperty("arangodb.port", "8529");
      int port = Integer.parseInt(portStr);

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
      arangoConfigure = new ArangoConfigure();
      arangoConfigure.setArangoHost(new ArangoHost(ip, port));
      arangoConfigure.init();
      arangoDriver = new ArangoDriver(arangoConfigure);

      // Init the database
      if (dropDBBeforeRun) {
        // Try delete first
        try {
          arangoDriver.deleteDatabase(databaseName);
        } catch (ArangoException e) {
          if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DATABASE_NOT_FOUND) {
            logger.error("Failed to delete database: " + databaseName, e);
            System.exit(-1);
          } else {
            logger.info("Fail to delete DB, already not exists: {}", databaseName);
          }
        }
      }
      try {
        arangoDriver.createDatabase(databaseName);
        logger.info("Database created: " + databaseName);
      } catch (ArangoException e) {
        if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DUPLICATE_NAME) {
          logger.error("Failed to create database: " + databaseName, e);
          System.exit(-1);
        } else {
          logger.info("DB already exists: {}", databaseName);
        }
      }
      // Always set the default db
      arangoDriver.setDefaultDatabase(databaseName);
      logger.info("ArangoDB client connection created to " + ip + ":" + port);
      
      // Log the configuration
      StringBuilder configStr = new StringBuilder("Arango Configuration: ");
      configStr.append(String.format("dropDBBeforeRun: %s; ", dropDBBeforeRun));
      configStr.append(String.format("address: %s:%s; ", ip, port));
      configStr.append(String.format("databaseName: %s; ", databaseName));
      configStr.append(String.format("waitForSync: %s; ", waitForSync));
      configStr.append(String.format("transactionUpdate: %s; ", transactionUpdate));
      logger.info(configStr.toString());
    }
  }

  /**
   * Can't create automatically during inserting, so create one if necessary.
   * @param table
   */
  private void createCollection(String table) {
    synchronized(ArangoDBClient.class) {
      if (defaultCollection == null) {
        try {
          CollectionOptions options = new CollectionOptions();
          options.setWaitForSync(waitForSync);
          arangoDriver.createCollection(table, options);
          logger.info("New collection created: {} waitForSync: {}", table, waitForSync);
        } catch (ArangoException e) {
          if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DUPLICATE_NAME) {
            logger.error("Fail to create collection", e);
            System.exit(-1);
          } else {
            logger.info("Collection already exists: {}", table);
          }
        }
        defaultCollection = table;
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
      arangoConfigure = null;
      arangoDriver = null;
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
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      if (defaultCollection == null) {
        createCollection(table);
      }
      BaseDocument toInsert = new BaseDocument(key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
      }
      arangoDriver.createDocument(table, toInsert, true/*create collection if not exist*/,
                                  waitForSync);
      return Status.OK;
    } catch (ArangoException e) {
      if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED) {
        logger.error("Fail to insert: " + table + " " + key,  e);
      } else {
        logger.debug("Trying to create document with duplicate key: {} {}", table, key);
        return Status.BAD_REQUEST;
      }
    }  catch (Exception e) {
      logger.error("Exception while trying insert " + table + " " + key, e);
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
  @SuppressWarnings("unchecked")
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    DocumentCursor<BaseDocument> cursor = null;
    try {
      DocumentEntity<BaseDocument> targetDoc = arangoDriver.getDocument(table, key, BaseDocument.class);
      BaseDocument aDocument = targetDoc.getEntity();
      this.fillMap(result, aDocument.getProperties(), fields);
      return Status.OK;
    } catch (ArangoException e) {
      if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
        logger.error("Fail to read: " + table + " " + key,  e);
      } else {
        logger.debug("Trying to read document not exist: {} {}", table, key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      logger.error("Exception while trying read " + table + " " + key, e);
    } finally {
      if (cursor != null) {
        try {
          cursor.close();
        } catch (ArangoException e) {
          logger.error("Fail to close cursor", e);
        }
      }
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
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      
      if (!transactionUpdate) {
        BaseDocument updateDoc = new BaseDocument();
        for (String field : values.keySet()) {
          updateDoc.addAttribute(field, byteIteratorToString(values.get(field)));
        }
        arangoDriver.updateDocument(table, key, updateDoc);
        return Status.OK;
      } else {
        // id for documentHandle
        String transactionAction = "function (id) {"
               // use internal database functions
            + "var db = require('internal').db;"
              // collection.update(document, data, overwrite, keepNull, waitForSync)
            + String.format("db._update(id, %s, true, false, %s);}",
                mapToJson(values), Boolean.toString(waitForSync).toLowerCase());
        TransactionEntity transaction = arangoDriver.createTransaction(transactionAction);
        transaction.addWriteCollection(table);
        transaction.setParams(createDocumentHandle(table, key));
        arangoDriver.executeTransaction(transaction);
        return Status.OK;
      }
    } catch (ArangoException e) {
      if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
        logger.error("Fail to update: " + table + " " + key,  e);
      } else {
        logger.debug("Trying to update document not exist: {} {}", table, key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      logger.error("Exception while trying update " + table + " " + key, e);
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
      arangoDriver.deleteDocument(table, key);
      return Status.OK;
    } catch (ArangoException e) {
      if (e.getErrorNumber() != ErrorNums.ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
        logger.error("Fail to delete: " + table + " " + key,  e);
      } else {
        logger.debug("Trying to delete document not exist: {} {}", table, key);
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      logger.error("Exception while trying delete " + table + " " + key, e);
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
    DocumentCursor<BaseDocument> cursor = null;
    try {
      String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
          recordcount, constructReturnForAQL(fields, "target"));

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
      cursor = arangoDriver.executeDocumentQuery(aqlQuery, bindVars, null, BaseDocument.class);
      Iterator<BaseDocument> iterator = cursor.entityIterator();
      while (iterator.hasNext()) {
        BaseDocument aDocument = iterator.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.getProperties().size());
        this.fillMap(aMap, aDocument.getProperties());
        result.add(aMap);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying scan " + table + " " + startkey + " " + recordcount, e);
    } finally {
      if (cursor != null) {
        try {
          cursor.close();
        } catch (ArangoException e) {
          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }

  private String createDocumentHandle(String collectionName, String documentKey) throws ArangoException {
    validateCollectionName(collectionName);
    return collectionName + "/" + documentKey;
  }

  private void validateCollectionName(String name) throws ArangoException {
    if (name.indexOf('/') != -1) {
      throw new ArangoException("does not allow '/' in name.");
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
  

  private void fillMap(Map<String, ByteIterator> resultMap, Map<String, Object> properties) {
    fillMap(resultMap, properties, null);
  }
  
  /**
   * Fills the map with the properties from the BaseDocument.
   * 
   * @param resultMap
   *      The map to fill/
   * @param obj
   *      The object to copy values from.
   */
  @SuppressWarnings("unchecked")
  private void fillMap(Map<String, ByteIterator> resultMap, Map<String, Object> properties, Set<String> fields) {
    if (fields == null || fields.size() == 0) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        if (entry.getValue() instanceof String) {
          resultMap.put(entry.getKey(),
              stringToByteIterator((String)(entry.getValue())));
        } else {
          logger.error("Error! Not the format expected! Actually is {}",
              entry.getValue().getClass().getName());
          System.exit(-1);
        }
      }
    } else {
      for (String field : fields) {
        if (properties.get(field) instanceof String) {
          resultMap.put(field, stringToByteIterator((String)(properties.get(field))));
        } else {
          logger.error("Error! Not the format expected! Actually is {}",
              properties.get(field).getClass().getName());
          System.exit(-1);
        }
      }
    }
  }
  
  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  private ByteIterator stringToByteIterator(String content) {
    return new StringByteIterator(content);
  }
  
  private String mapToJson(HashMap<String, ByteIterator> values) {
    HashMap<String, String> intervalRst = new HashMap<String, String>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      intervalRst.put(entry.getKey(), byteIteratorToString(entry.getValue()));
    }
    return EntityFactory.toJsonString(intervalRst);
  }
  
}
