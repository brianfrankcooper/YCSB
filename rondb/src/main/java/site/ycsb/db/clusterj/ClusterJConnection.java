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

import com.mysql.clusterj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.DBException;
import site.ycsb.db.ConfigKeys;
import site.ycsb.db.clusterj.table.ClassGenerator;
import site.ycsb.db.clusterj.table.UserTableHelper;

import java.util.Properties;

/**
 * This is the ClusterJ connection for RonDB.
 */
public final class ClusterJConnection {
  protected static Logger logger = LoggerFactory.getLogger(ClusterJConnection.class);
  private static Object lock = new Object();
  protected static ClassGenerator classGenerator = new ClassGenerator();
  private static SessionFactory sessionFactory;
  private static ClusterJConnection connection = null;


  private ClusterJConnection() {
  }

  public static ClusterJConnection connect(Properties properties) throws DBException {
    synchronized (lock) {
      if (connection != null) {
        return connection;
      } else {
        connection = new ClusterJConnection(properties);
        return connection;
      }
    }
  }

  private ClusterJConnection(Properties properties) throws DBException {
    if (sessionFactory == null) {
      sessionFactory = setUpDBConnection(properties);
    }
  }

  void releaseSession(Session session) {
    session.closeCache();
  }

  void releaseDTO(Session session, DynamicObject dto) {
    session.releaseCache(dto, dto.getClass());
  }

  Class<DynamicObject> getDTOClass(String tableName) {
    try {
      @SuppressWarnings("unchecked")
      Class<DynamicObject> tableClass = (Class<DynamicObject>)
          UserTableHelper.getTableClass(classGenerator, tableName);
      return tableClass;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private SessionFactory setUpDBConnection(Properties properties) throws DBException {
    String connString = properties.getProperty(ConfigKeys.CONNECT_STR_KEY,
        ConfigKeys.CONNECT_STR_DEFAULT);
    String schema = properties.getProperty(ConfigKeys.SCHEMA_KEY, ConfigKeys.SCHEMA_DEFAULT);
    logger.info("Connecting to schema: " + schema + " on " + connString + ".");

    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", connString);
    props.setProperty("com.mysql.clusterj.database", schema);
    props.setProperty("com.mysql.clusterj.connect.retries", "3");
    props.setProperty("com.mysql.clusterj.connect.delay", "4");
    props.setProperty("com.mysql.clusterj.connect.verbose", "0");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "29");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "19");
    props.setProperty("com.mysql.clusterj.max.transactions", "1023");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    props.setProperty("com.mysql.clusterj.max.cached.instances", "1024");
    props.setProperty("com.mysql.clusterj.max.cached.sessions", "1024");
    //props.setProperty("com.mysql.clusterj.connection.pool.recv.thread.activation.threshold", "7");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw new DBException(ex);
    }
    System.out.println("Connected to RonDB");
    return sessionFactory;
  }

  public Session getSession() {
    return sessionFactory.getSession();
  }

  public static synchronized void shutDown(){
    if (sessionFactory != null) {
      sessionFactory.close();
      sessionFactory = null;
    }
  }
}
