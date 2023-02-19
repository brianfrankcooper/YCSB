/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
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
import site.ycsb.db.http.GrpcClient;
import site.ycsb.db.http.RestApiClient;
import site.ycsb.workloads.CoreWorkload;

import java.io.IOException;
import java.util.*;

/**
 * This is the REST API client for RonDB.
 */
public class RonDBClient extends DB {
  protected static Logger logger = LoggerFactory.getLogger(RonDBClient.class);
  private ClusterJClient clusterJClient;
  private RestApiClient restApiClient;
  private GrpcClient grpcClient;

  private static Object lock = new Object();

  private static final String RONDB_API_TYPE_PROP = "rondb.api.type";
  private static RonDBAPIType ronDBAPIType;
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

      String apiPropStr = properties.getProperty(RONDB_API_TYPE_PROP, RonDBAPIType.CLUSTERJ.toString());

      try {
        if (apiPropStr.compareToIgnoreCase(RonDBAPIType.CLUSTERJ.toString()) == 0) {
          ronDBAPIType = RonDBAPIType.CLUSTERJ;
          clusterJClient = new ClusterJClient(properties);
        } else if (apiPropStr.compareToIgnoreCase(RonDBAPIType.REST.toString()) == 0) {
          ronDBAPIType = RonDBAPIType.REST;
          restApiClient = new RestApiClient(properties);
        } else if (apiPropStr.compareToIgnoreCase(RonDBAPIType.GRPC.toString()) == 0) {
          ronDBAPIType = RonDBAPIType.GRPC;
          grpcClient = new GrpcClient(properties);
        } else {
          throw new IllegalArgumentException("Wrong argument " + RONDB_API_TYPE_PROP);
        }
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
        throw new DBException("Failed to initialize. " + e);
      }

      fieldCount = Long.parseLong(
          properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      final String fieldnameprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
          CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
      fieldNames = new HashSet<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add(fieldnameprefix + i);
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
      if (ronDBAPIType == RonDBAPIType.REST) {
        return restApiClient.read(threadID, table, key, fieldsToRead, result);
      } else if (ronDBAPIType == RonDBAPIType.GRPC) {
        return grpcClient.read(threadID, table, key, fieldsToRead, result);
      } else if (ronDBAPIType == RonDBAPIType.CLUSTERJ) {
        return clusterJClient.read(table, key, fieldsToRead, result);
      } else {
        throw new UnsupportedOperationException("Read operation not supported for " + ronDBAPIType);
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
    try {
      if (ronDBAPIType == RonDBAPIType.REST) {
        throw new UnsupportedOperationException("Scan operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.GRPC) {
        throw new UnsupportedOperationException("Scan operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.CLUSTERJ) {
        return clusterJClient.scan(table, startkey, recordcount, fieldsToRead, result);
      } else {
        throw new UnsupportedOperationException("Scan operation not supported for " + ronDBAPIType);
      }
    } catch (Exception e) {
      logger.error("Error " + e);
      return Status.ERROR;
    }
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
    try {
      if (ronDBAPIType == RonDBAPIType.REST) {
        throw new UnsupportedOperationException("Update operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.GRPC) {
        throw new UnsupportedOperationException("Update operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.CLUSTERJ) {
        return clusterJClient.update(table, key, values);
      } else {
        throw new UnsupportedOperationException("Update Operation not supported for " + ronDBAPIType);
      }
    } catch (Exception e) {
      logger.error("Error " + e);
      return Status.ERROR;
    }
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
    try {
      if (ronDBAPIType == RonDBAPIType.REST) {
        throw new UnsupportedOperationException("Insert operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.GRPC) {
        throw new UnsupportedOperationException("Insert operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.CLUSTERJ) {
        return clusterJClient.insert(table, key, values);
      } else {
        throw new UnsupportedOperationException("Insert Operation not supported for " + ronDBAPIType);
      }
    } catch (Exception e) {
      logger.error("Error " + e);
      return Status.ERROR;
    }
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
    try {
      if (ronDBAPIType == RonDBAPIType.REST) {
        throw new UnsupportedOperationException("Delete operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.GRPC) {
        throw new UnsupportedOperationException("Delete operation not supported for " + ronDBAPIType);
      } else if (ronDBAPIType == RonDBAPIType.CLUSTERJ) {
        return clusterJClient.delete(table, key);
      } else {
        throw new UnsupportedOperationException("Delete Operation not supported for " + ronDBAPIType);
      }
    } catch (Exception e) {
      logger.error("Error " + e);
      return Status.ERROR;
    }
  }

  public static Logger getLogger() {
    return logger;
  }
}
