/*
  Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {
  /** The driver to get the connection to postgresql. */
  private static Driver postgrenosqlDriver;

  /** The connection to the database. */
  private static Connection connection;

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "postgrenosql.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "postgrenosql.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "postgrenosql.passwd";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "postgresql.autocommit";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  private boolean initialized = false;
  private boolean autoCommit;
  private Properties props;

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

    try{
      Properties tmpProps = new Properties();
      tmpProps.setProperty("user", user);
      tmpProps.setProperty("password", passwd);

      postgrenosqlDriver = new Driver();
      connection = postgrenosqlDriver.connect(urls, props);
      connection.setAutoCommit(autoCommit);
    } catch (Exception e){
      System.err.println("Error during initialization: " + e);
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try{
      PreparedStatement readStatement = connection.prepareStatement(createReadStatement(tableName, fields));
      readStatement.setString(1, key);

      System.out.println(readStatement);

      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return  Status.NOT_FOUND;
      }

      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return Status.OK;

    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(tableName, fields));

      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();

      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = (String) resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }

          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    try{
      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(tableName));

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      updateStatement.setObject(1, object);
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    try{
      PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(tableName));
      insertStatement.setString(1, key);

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());
      insertStatement.setObject(2, object);

      int result = insertStatement.executeUpdate();

      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try{
      PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(tableName));
      deleteStatement.setString(1, key);


      int result = deleteStatement.executeUpdate();

      if (result == 1){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private String createReadStatement(String table, Set<String> fields) {
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (fields != null){
      for (String field:fields){
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS" + field);
      }
    }
    read.append(" FROM " + table);
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  private String createScanStatement(String table, Set<String> fields) {
    StringBuilder select = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (fields != null){
      for (String field:fields){
        select.append(", " + COLUMN_NAME + "->>'" + field + "' AS" + field);
      }
    }
    select.append(" FROM " + table);
    select.append(" WHERE ");
    select.append(PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(PRIMARY_KEY);
    select.append(" LIMIT ?");
    return select.toString();
  }

  public String createUpdateStatement(String table) {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(table);
    update.append(" SET ");
    update.append(COLUMN_NAME + " = " + COLUMN_NAME);
    update.append(" || ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private String createInsertStatement(String table) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(table);
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private String createDeleteStatement(String table) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(table);
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
