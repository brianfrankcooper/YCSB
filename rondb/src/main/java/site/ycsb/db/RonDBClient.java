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

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.table.ClassGenerator;
import site.ycsb.db.table.UserTableHelper;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */
public class RonDBClient extends DB {
  private static Logger logger = LoggerFactory.getLogger(RonDBClient.class);
  private static RonDBConnection connection;
  private static Object lock = new Object();
  private static ClassGenerator classGenerator = new ClassGenerator();
  private String tableName = "usertable";
  private long fieldCount = 1;
  private Set<String> fieldNames;


  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    synchronized (lock) {
      fieldCount =
          Long.parseLong(getProperties().getProperty(CoreWorkload.FIELD_COUNT_PROPERTY,
              CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      final String fieldnameprefix = getProperties().getProperty(CoreWorkload.FIELD_NAME_PREFIX,
          CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
      fieldNames = new HashSet<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add(fieldnameprefix + i);
      }
      tableName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY);
      if (tableName == null) {
        tableName = CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
      }
      //logger.info("Settings: Table: " + tableName + " Field Count: " + fieldCount +
      //    " Fields: " + Arrays.toString(fieldNames.toArray()));

      if (connection == null) {
        connection = RonDBConnection.connect(getProperties());
      }
      Session session = connection.getSession(); //initialize session for this thread
      connection.returnSession(session);
      System.out.println("Created a session for the thread");
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    synchronized (lock) {
      if (connection != null) {
        RonDBConnection.closeSession(connection);
      }
    }
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
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Session session = connection.getSession();
    try {

      Class<DynamicObject> dbClass = (Class<DynamicObject>) UserTableHelper.getTableClass(
          classGenerator, tableName);
      DynamicObject row = session.find(dbClass, key);
      if (row == null) {
        logger.info("Read. Key: " + key + " Not Found.");
        return Status.NOT_FOUND;
      }
      Set<String> toRead = fields != null ? fields : fieldNames;
      for (String field : toRead) {
        result.put(field, UserTableHelper.readFieldFromDTO(field, row));
      }
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Read Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Read Error: " + e);
        return Status.ERROR;
      }
      return Status.OK; // session is closing
    } finally {
      connection.returnSession(session);
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
    Session session = connection.getSession();
    try {
      Class<DynamicObject> dbClass = (Class<DynamicObject>) UserTableHelper.getTableClass(
          classGenerator, tableName);
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<DynamicObject> dobj =
          qb.createQueryDefinition(dbClass);
      Predicate pred1 = dobj.get(UserTableHelper.KEY).greaterEqual(dobj.param(UserTableHelper.KEY +
          "Param"));
      dobj.where(pred1);
      Query<DynamicObject> query = session.createQuery(dobj);
      query.setParameter(UserTableHelper.KEY + "Param", startkey);
      query.setLimits(0, recordcount);
      List<DynamicObject> scanResults = query.getResultList();
      for (DynamicObject dto : scanResults) {
        result.add(UserTableHelper.readFieldsFromDTO(dto, fields != null ? fields : fieldNames));
        releaseDTO(session, dto);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Scan. Rows returned: " + result.size());
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Scan Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values
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
    Session session = connection.getSession();
    try {
      DynamicObject row = UserTableHelper.createDTO(classGenerator, session, tableName,
          key, values);
      session.savePersistent(row);
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Updated Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Update Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Session session = connection.getSession();
    try {
      DynamicObject row = UserTableHelper.createDTO(classGenerator, session, tableName,
          key, values);
      session.savePersistent(row);
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Inserted Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Insert Error: " + e);
        e.printStackTrace();
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
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
    Session session = connection.getSession();
    try {
      DynamicObject row = UserTableHelper.createDTO(classGenerator, session, tableName,
          key, null);
      session.deletePersistent(row);
      releaseDTO(session, row);
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Delete Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  private void releaseDTO(Session session, DynamicObject dto) {
    session.releaseCache(dto, dto.getClass());
  }

  private boolean isSessionClosing(Exception e) {
    if (e instanceof ClusterJException && e.getMessage().contains("Db is closing")) {
      return true;
    }
    return false;
  }

}
