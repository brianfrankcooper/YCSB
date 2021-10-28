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
package site.ycsb.db;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.DBException;

import java.util.Properties;

/**
 * RonDB Connection object.
 */
public final class RonDBConnection {

  private static Logger logger = LoggerFactory.getLogger(RonDBConnection.class);

  private static final String HOST_PROPERTY = "rondb.host";
  private static final String PORT_PROPERTY = "rondb.port";
  private static final String SCHEMA = "rondb.schema";
  private static SessionFactory sessionFactory;
  private static ThreadLocal<Session> sessions = new ThreadLocal<>();


  private RonDBConnection() {
  }

  static synchronized RonDBConnection connect(Properties props) throws DBException {
    String port = props.getProperty(PORT_PROPERTY);
    if (port == null) {
      port = "1186";
    }
    String host = props.getProperty(HOST_PROPERTY);
    if (host == null) {
      host = "127.0.0.1";
    }
    String schema = props.getProperty(SCHEMA);
    if (schema == null) {
      schema = "ycsb";
    }

    RonDBConnection connection = new RonDBConnection();
    connection.setUpDBConnection(host, port, schema);
    return connection;
  }

  public void setUpDBConnection(String host, String port, String schema) throws DBException {
    logger.info("Connecting to  schema: " + schema + " on " + host + ":" + port + ".");

    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", host + ":" + port);
    props.setProperty("com.mysql.clusterj.database", schema);
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "4");
    props.setProperty("com.mysql.clusterj.max.cached.instances", "256");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw new DBException(ex);
    }
    System.out.println("Connected to RonDB");
  }

  public static synchronized void closeSession(RonDBConnection connection) {
    Session session = connection.getSession();
    session.close();
    sessions.set(null);
  }

  public Session getSession() {
    Session session = sessions.get();
    if (session == null) {
      session = sessionFactory.getSession();
      sessions.set(session);
    }

    return session;
  }

  public void returnSession(Session session) {
    // do not close the session. the same session will be
    // returned if needed again.
  }
}
