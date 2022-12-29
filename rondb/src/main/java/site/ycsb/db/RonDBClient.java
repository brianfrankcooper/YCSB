/*
 * Copyright (c) 2021, Yahoo!, Inc. All rights reserved.
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
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */

/**
 * RonDB client binding for YCSB.
 */

package site.ycsb.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.clusterj.ClusterJClient;
import site.ycsb.db.http.RestApiClient;
import site.ycsb.db.http.GrpcClient;
import site.ycsb.workloads.CoreWorkload;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Properties;

/**
 * This is the REST API client for RonDB.
 */
public class RonDBClient extends DB {
  protected static Logger logger = LoggerFactory.getLogger(RonDBClient.class);
  private ClusterJClient clusterJClient;
  private RestApiClient restApiClient;
  private GrpcClient grpcClient;

  private static Object lock = new Object();

  private static final String RONDB_USE_GRPC = "rondb.use.grpc";
  private static final String RONDB_USE_REST_API = "rondb.use.rest.api";
  private static boolean useRESTAPI;
  private static boolean useGRPC;
  private long fieldCount;
  private Set<String> fieldNames;
  private static int maxThreadID = 0;
  private int threadID = 0;

  public RonDBClient() {
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    Properties properties = getProperties();
    synchronized (lock) {
      threadID = maxThreadID++;

      useRESTAPI = Boolean.parseBoolean(properties.getProperty(RONDB_USE_REST_API, "false"));
      useGRPC = Boolean.parseBoolean(properties.getProperty(RONDB_USE_GRPC, "false"));
      if (useRESTAPI && useGRPC) {
        logger.error("cannot use both REST API and GRPC");
        System.exit(1);
      }

      // It can apparently happen that the methods omit the parameter "fields", so
      // we're just saving it here
      fieldCount = Long.parseLong(
          properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      final String fieldnameprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
          CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
      fieldNames = new HashSet<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add(fieldnameprefix + i);
      }
    }

    clusterJClient = new ClusterJClient(properties);
    if (useRESTAPI) {
      try {
        restApiClient = new RestApiClient(properties);
      } catch (IOException e) {
        logger.error("error creating RonDB REST API client " + e);
        e.printStackTrace();
        System.exit(1);
      }
    } else if (useGRPC) {
      try {
        grpcClient = new GrpcClient(properties);
      } catch (IOException e) {
        logger.error("error creating RonDB gRPC client " + e);
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    ClusterJClient.cleanup();
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Set<String> fieldsToRead = fields != null ? fields : fieldNames;
    try {
      if (useRESTAPI) {
        return restApiClient.read(threadID, table, key, fieldsToRead, result);
      } else if (useGRPC) {
        return grpcClient.read(threadID, table, key, fieldsToRead, result);
      } else {
        return clusterJClient.read(table, key, fieldsToRead, result);
      }
    } catch (Exception e) {
      logger.error("Error " + e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set
   *                    field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Set<String> fieldsToRead = fields != null ? fields : fieldNames;
    return clusterJClient.scan(table, startkey, recordcount, fieldsToRead, result);
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values
   * HashMap will be written into the record with the specified record key,
   * overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return clusterJClient.update(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values
   * HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return clusterJClient.insert(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return The result of the operation.
   */
  @Override
  public Status delete(String table, String key) {
    return clusterJClient.delete(table, key);
  }

  public static Logger getLogger() {
    return logger;
  }
}
