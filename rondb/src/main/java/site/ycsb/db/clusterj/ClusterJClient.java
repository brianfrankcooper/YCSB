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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.clusterj.table.UserTableHelper;
import site.ycsb.db.clusterj.tx.TransactionReqHandler;
import site.ycsb.workloads.CoreWorkload;

import java.util.*;

/**
 * This is the ClusterJ client for RonDB.
 */
public final class ClusterJClient extends DB {
  protected static Logger logger = LoggerFactory.getLogger(ClusterJClient.class);
  private static Object lock = new Object();
  private static ClusterJConnection connection;
  private Properties properties;

  public ClusterJClient(Properties properties) throws DBException {
    this.properties = properties;
  }

  @Override
  public void init() throws DBException {
    synchronized (lock) {
      if (connection == null) {
        connection = ClusterJConnection.connect(properties);
      }

      //warmup
      String tableName = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY,
          CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      Session session = connection.getSession(); // initialize session for this thread
      try {
        DynamicObject persistable = UserTableHelper.getTableObject(ClusterJConnection.classGenerator,
            session, tableName);
        connection.releaseDTO(session, persistable);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        System.exit(1);
      }
      connection.releaseSession(session);
    }
  }

  @Override
  public void cleanup() throws DBException {
    ClusterJConnection.shutDown();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Class<DynamicObject> dbClass = connection.getDTOClass(table);
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
          connection.releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Read Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.releaseSession(session);
    }
  }

  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          HashMap<String, HashMap<String, ByteIterator>> results) {
    Class<DynamicObject> dbClass = connection.getDTOClass(table);
    final Session session = connection.getSession();
    try {
      TransactionReqHandler handler = new TransactionReqHandler("BatchRead") {
        @Override
        public Status action() throws Exception {

          List<DynamicObject> rows = new ArrayList<>(keys.size());
          for (int i = 0; i < keys.size(); i++) {
            String pk = keys.get(i);
            DynamicObject row = session.newInstance(dbClass);
            UserTableHelper.setPK(pk, row);
            session.load(row);
            rows.add(row);
          }
          session.flush();

          for (int i = 0; i < keys.size(); i++) {
            DynamicObject row = rows.get(i);
            String pk = keys.get(i);

            Set<String> rowFields = fields.get(i);
            HashMap<String, ByteIterator> rowResult = results.get(pk);
            for (String field : rowFields) {
              rowResult.put(field, UserTableHelper.readFieldFromDTO(field, row));
            }
          }

          for (int i = 0; i < keys.size(); i++) {
            DynamicObject row = rows.get(i);
            connection.releaseDTO(session, row);
          }

          if (logger.isDebugEnabled()) {
            logger.debug("BatchRead " + keys.size() + " keys ");
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, keys.get(0)/*use first key as partition key*/);
    } finally {
      connection.releaseSession(session);
    }
  }

  @Override
  public Status delete(String table, String key) {
    Class<DynamicObject> dbClass;
    final Session session = connection.getSession();
    dbClass = connection.getDTOClass(table);

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Delete") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(ClusterJConnection.classGenerator,
              session, table, key, null);
          session.deletePersistent(row);
          connection.releaseDTO(session, row);
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.releaseSession(session);
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    Class<DynamicObject> dbClass = connection.getDTOClass(table);
    final Session session = connection.getSession();

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Scan") {
        @Override
        public Status action() throws Exception {
          QueryBuilder qb = session.getQueryBuilder();
          QueryDomainType<DynamicObject> dobj = qb.createQueryDefinition(dbClass);
          Predicate pred1 = dobj.get(UserTableHelper.KEY).
              greaterEqual(dobj.param(UserTableHelper.KEY + "Param"));
          dobj.where(pred1);
          Query<DynamicObject> query = session.createQuery(dobj);
          query.setParameter(UserTableHelper.KEY + "Param", startkey);
          query.setLimits(0, recordcount);
          List<DynamicObject> scanResults = query.getResultList();
          for (DynamicObject dto : scanResults) {
            result.add(UserTableHelper.readFieldsFromDTO(dto, fields));
            connection.releaseDTO(session, dto);
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Scan. Rows returned: " + result.size());
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, startkey);
    } finally {
      connection.releaseSession(session);
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Class<DynamicObject> dbClass = connection.getDTOClass(table);
    final Session session = connection.getSession();

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Update") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(connection.classGenerator, session, table
              , key,
              values);
          session.savePersistent(row);
          connection.releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Updated Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.releaseSession(session);
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Class<DynamicObject> dbClass;
    final Session session = connection.getSession();
    dbClass = connection.getDTOClass(table);

    try {
      TransactionReqHandler handler = new TransactionReqHandler("Insert") {
        @Override
        public Status action() throws Exception {
          DynamicObject row = UserTableHelper.createDTO(ClusterJConnection.classGenerator,
              session, table, key, values);
          session.makePersistent(row);
          connection.releaseDTO(session, row);
          if (logger.isDebugEnabled()) {
            logger.debug("Inserted Key " + key);
          }
          return Status.OK;
        }
      };
      return handler.runTx(session, dbClass, key);
    } finally {
      connection.releaseSession(session);
    }
  }
}
