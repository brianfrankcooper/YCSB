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
 * RonDB client binding for YCSB.
 */
package site.ycsb.db.clusterj;

import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.DBException;
import site.ycsb.db.clusterj.table.ClassGenerator;
import site.ycsb.db.clusterj.table.UserTableHelper;
import site.ycsb.db.clusterj.tx.TransactionReqHandler;
import site.ycsb.workloads.CoreWorkload;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.HashMap;

/**
 * This is the ClusterJ client for RonDB.
 */
public final class ClusterJClient {
  protected static Logger logger = LoggerFactory.getLogger(ClusterJClient.class);
  private static Object lock = new Object();
  private static RonDBConnection connection;
  private static ClassGenerator classGenerator = new ClassGenerator();

  public ClusterJClient(Properties properties) throws DBException {

    // Setting static class properties in parallel
    synchronized (lock) {
      if (connection == null) {
        connection = RonDBConnection.connect(properties);
      }

      // TODO: Add a comment what this does
      String tableName = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY,
          CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      Session session = connection.getSession(); // initialize session for this thread
      for (int i = 0; i < 1024; i++) {
        try {
          DynamicObject persistable = UserTableHelper.getTableObject(classGenerator, session, tableName);
          releaseDTO(session, persistable);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
      connection.returnSession(session);
    }
  }

  public static synchronized void cleanup() throws DBException {
    if (connection != null) {
      RonDBConnection.closeSession(connection);
    }
  }

  private static void releaseDTO(Session session, DynamicObject dto) {
    session.releaseCache(dto, dto.getClass());
  }

  /*
   * private boolean isSessionClosing(Exception e) {
   * if (e instanceof ClusterJException &&
   * e.getMessage().contains("Db is closing")) {
   * return true;
   * }
   * return false;
   * }
   */

  private static Class<DynamicObject> getDTOClass(String tableName) {
    try {
      @SuppressWarnings("unchecked")
      Class<DynamicObject> tableClass = (Class<DynamicObject>) UserTableHelper.getTableClass(classGenerator, tableName);
      return tableClass;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Class<DynamicObject> dbClass = getDTOClass(table);
    final Session session = connection.getSession();
    try {
      TransactionReqHandler handler = new TransactionReqHandler("Read") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = session.find(dbClass, key);
          if (row == null) {
            logger.info("Read. Key: " + key + " Not Found.");
            return Status.NOT_FOUND;
          }
          for (String field : fields) {
            result.put(field, UserTableHelper.readFieldFromDTO(field, row));
          }
          releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Read Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.returnSession(session);
    }
  }

  public Status delete(String table, String key) {
    Class<DynamicObject> dbClass;
    final Session session = connection.getSession();
    dbClass = getDTOClass(table);

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Delete") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(classGenerator, session, table, key, null);
          session.deletePersistent(row);
          releaseDTO(session, row);
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.returnSession(session);
    }
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    Class<DynamicObject> dbClass = getDTOClass(table);
    final Session session = connection.getSession();

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Scan") {
        @Override
        public Status action() throws Exception {
          QueryBuilder qb = session.getQueryBuilder();
          QueryDomainType<DynamicObject> dobj = qb.createQueryDefinition(dbClass);
          Predicate pred1 = dobj.get(UserTableHelper.KEY).greaterEqual(dobj.param(UserTableHelper.KEY +
              "Param"));
          dobj.where(pred1);
          Query<DynamicObject> query = session.createQuery(dobj);
          query.setParameter(UserTableHelper.KEY + "Param", startkey);
          query.setLimits(0, recordcount);
          List<DynamicObject> scanResults = query.getResultList();
          for (DynamicObject dto : scanResults) {
            result.add(UserTableHelper.readFieldsFromDTO(dto, fields));
            releaseDTO(session, dto);
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Scan. Rows returned: " + result.size());
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, startkey);
    } finally {
      connection.returnSession(session);
    }
  }

  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Class<DynamicObject> dbClass = getDTOClass(table);
    final Session session = connection.getSession();

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Update") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(classGenerator, session, table, key, values);
          session.savePersistent(row);
          releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Updated Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.returnSession(session);
    }
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Class<DynamicObject> dbClass;
    final Session session = connection.getSession();
    dbClass = getDTOClass(table);

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Insert") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(classGenerator, session, table, key, values);
          session.makePersistent(row);
          releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Inserted Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.returnSession(session);
    }
  }

}
