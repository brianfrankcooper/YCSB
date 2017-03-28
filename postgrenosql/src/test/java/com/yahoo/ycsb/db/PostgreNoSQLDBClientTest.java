/*
  Copyright (c) 2010 - 2016 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.

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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

/**
 * PostgreNoSQL test client for YCSB framework.
 */
public class PostgreNoSQLDBClientTest {
  /** The default port for PostgreSQL. */
  private static final int DEFAULT_PORT = 5430;
  private static final String DEFAULT_USER = "postgres";
  private static final String DEFAULT_PWD = "postgres";

  /** The properties settings */
  private static final String TEST_DB_DRIVER = "com.yahoo.ycsb.db.PostgreNoSQLDBClient";
  private static final String TEST_DB_URL = "jdbc:postgresql://localhost:5430/test";
  private static final String DATABASE_NAME = "test";
  private static final String TABLE_NAME = "usertable";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX = "user";
  private static final String KEY_FIELD = "YCSB_KEY";
  private static final int NUM_FIELDS = 3;

  private static Connection postgreSQLConnection = null;
  private static PostgreNoSQLDBClient postgreNoSQLClient = null;

  @BeforeClass
  public static void setUp() {
    Properties props = new Properties();
    props.setProperty(PostgreNoSQLDBClient.CONNECTION_URL, TEST_DB_URL);
    props.setProperty(PostgreNoSQLDBClient.CONNECTION_USER, DEFAULT_USER);
    props.setProperty(PostgreNoSQLDBClient.CONNECTION_PASSWD, DEFAULT_PWD);
    props.setProperty("user", DEFAULT_USER);
    props.setProperty("password", DEFAULT_PWD);
    props.setProperty(PostgreNoSQLDBClient.JDBC_AUTO_COMMIT, "true");

    try{
      postgreSQLConnection = new Driver().connect(TEST_DB_URL, props);
      deleteTable();
      createTable();

      postgreNoSQLClient = new PostgreNoSQLDBClient();
      postgreNoSQLClient.setProperties(props);
      postgreNoSQLClient.init();
    }
    catch (SQLException | DBException e){
      System.err.println(e);
    }
  }

  @AfterClass
  public static void tearDown() {
    deleteTable();
  }

  @Test
  public void insertRead() {
    String insertKey = "user0";
    try{
      HashMap<String, ByteIterator> insertMap = new HashMap<>();
      HashMap<String, ByteIterator> copiedInsertMap = new HashMap<>();
      Set<String> fields = createFieldSet();

      for (int i = 0; i < NUM_FIELDS; i++) {
        byte[] value = new byte[10];
        for (int j = 0;j < value.length;j++){
          value[j] = (byte)((i+1)*(j+1));
        }

        insertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        copiedInsertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        fields.add(FIELD_PREFIX + i);
      }

      Status result = postgreNoSQLClient.insert(TABLE_NAME, insertKey, insertMap);
      assertThat("Insert did not return success (0).", result, is(Status.OK));

      HashMap<String, ByteIterator> readResults = new HashMap<>();
      result = postgreNoSQLClient.read(TABLE_NAME, insertKey, fields, readResults);
      assertThat("Read did not return success (0).", result, is(Status.OK));

      for (Map.Entry<String, ByteIterator> entry : readResults.entrySet()) {
        assertArrayEquals("Read result does not match wrote entries.", entry.getValue().toArray(), copiedInsertMap.get(entry.getKey()).toArray());
      }
    } catch (Exception e){
      System.err.println(e);
    }
  }

  @Test
  public void insertReadDelete() {
    String insertKey = "user1";
    try{
      HashMap<String, ByteIterator> insertMap = new HashMap<>();
      HashMap<String, ByteIterator> copiedInsertMap = new HashMap<>();
      Set<String> fields = createFieldSet();

      for (int i = 0; i < NUM_FIELDS; i++) {
        byte[] value = new byte[10];
        for (int j = 0;j < value.length;j++){
          value[j] = (byte)((i+1)*(j+1));
        }

        insertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        copiedInsertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        fields.add(FIELD_PREFIX + i);
      }

      Status result = postgreNoSQLClient.insert(TABLE_NAME, insertKey, insertMap);
      assertThat("Insert did not return success (0).", result, is(Status.OK));

      HashMap<String, ByteIterator> readResults = new HashMap<>();
      result = postgreNoSQLClient.read(TABLE_NAME, insertKey, fields, readResults);
      assertThat("Read did not return success (0).", result, is(Status.OK));

      for (Map.Entry<String, ByteIterator> entry : readResults.entrySet()) {
        assertArrayEquals("Read result does not match wrote entries.", entry.getValue().toArray(), copiedInsertMap.get(entry.getKey()).toArray());
      }

      result = postgreNoSQLClient.delete(TABLE_NAME, insertKey);
      assertThat("Delete did not return success (0).", result, is(Status.OK));

      result = postgreNoSQLClient.read(TABLE_NAME, insertKey, fields, readResults);
      assertThat("Read did not return not found (0).", result, is(Status.NOT_FOUND));

    } catch (Exception e){
      System.err.println(e);
    }
  }

  @Test
  public void insertScan() {
    int numberOfValuesToInsert = 100;
    int recordcount = 5;
    String startKey = "00050";

    // create set of fields to scan
    Set<String> fields = createFieldSet();

    // create values to insert
    for (int i = 0;i < numberOfValuesToInsert;i++){
      HashMap<String, ByteIterator> insertMap = new HashMap<>();

      for (int j = 0; j < NUM_FIELDS; j++) {
        byte[] value = new byte[10];
        for (int k = 0; k < value.length; k++) {
          value[k] = (byte) ((j + 1) * (k + 1));
        }

        insertMap.put(FIELD_PREFIX + j, new ByteArrayByteIterator(value));
      }

      postgreNoSQLClient.insert(TABLE_NAME, padded(i, 5), insertMap);
    }

    Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    Status result = postgreNoSQLClient.scan(TABLE_NAME, startKey,recordcount, fields, results);
    assertThat("Scan did not return success (0).", result, is(Status.OK));
    assertThat("Number of results does not match.", results.size(), is(recordcount));
  }

  @Test
  public void insertUpdate(){
    String insertKey = "user2";
    try{
      HashMap<String, ByteIterator> insertMap = new HashMap<>();
      HashMap<String, ByteIterator> copiedInsertMap = new HashMap<>();
      Set<String> fields = createFieldSet();

      for (int i = 0; i < NUM_FIELDS; i++) {
        byte[] value = new byte[10];
        for (int j = 0;j < value.length;j++){
          value[j] = (byte)((i+1)*(j+1));
        }

        insertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        copiedInsertMap.put(FIELD_PREFIX + i, new ByteArrayByteIterator(value));
        fields.add(FIELD_PREFIX + i);
      }

      Status result = postgreNoSQLClient.insert(TABLE_NAME, insertKey, insertMap);
      assertThat("Insert did not return success (0).", result, is(Status.OK));

      HashMap<String, ByteIterator> updateMap = new HashMap<>();
      updateMap.put("FIELD0", new ByteArrayByteIterator(new byte[]{99, 99, 99, 99}));

      result = postgreNoSQLClient.update(TABLE_NAME, insertKey, updateMap);
      assertThat("Update did not return success (0).", result, is(Status.OK));

      HashMap<String, ByteIterator> readResults = new HashMap<>();
      result = postgreNoSQLClient.read(TABLE_NAME, insertKey, fields, readResults);
      assertThat("Read did not return success (0).", result, is(Status.OK));
      assertThat("Value was not updated correctly.", readResults.get("FIELD0").toArray(), is(new byte[]{99, 99, 99, 99}));

    } catch (Exception e){
      System.err.println(e);
    }
  }

  private static void createTable(){
    if (null != postgreSQLConnection){
      System.out.println("createTable");
      try{
        // create sql command
        StringBuilder sqlCommand = new StringBuilder("CREATE TABLE");
        sqlCommand.append(" ");
        sqlCommand.append(TABLE_NAME);
        sqlCommand.append(" ");
        sqlCommand.append("(");
        sqlCommand.append(PostgreNoSQLDBClient.PRIMARY_KEY + " VARCHAR(255) PRIMARY KEY not NULL");
        sqlCommand.append(",");
        sqlCommand.append(PostgreNoSQLDBClient.COLUMN_NAME + " JSONB not NULL");
        sqlCommand.append(")");

        // create table
        PreparedStatement statement = postgreSQLConnection.prepareStatement(sqlCommand.toString());
        statement.execute();
      } catch (SQLException e){
        System.err.println(e);
      }
    }
  }

  private static void deleteTable(){
    if (null != postgreSQLConnection){
      try{
        // create sql command
        StringBuilder sqlCommand = new StringBuilder("DROP TABLE IF EXISTS");
        sqlCommand.append(" ");
        sqlCommand.append(TABLE_NAME);

        // delete table
        PreparedStatement statement = postgreSQLConnection.prepareStatement(sqlCommand.toString());
        System.out.println(statement);
        statement.execute();
      } catch (SQLException e){
        System.err.println(e);
      }
    }
  }

  private String padded(int i, int padding) {
    String result = String.valueOf(i);
    while (result.length() < padding) {
      result = "0" + result;
    }
    return result;
  }

  private Set<String> createFieldSet()
  {
    Set<String> fields = new HashSet<>();
    for (int j = 0; j < NUM_FIELDS; j++) {
      fields.add(FIELD_PREFIX + j);
    }
    return fields;
  }
}
