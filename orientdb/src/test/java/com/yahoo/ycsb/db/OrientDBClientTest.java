/**
 * Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import org.junit.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by kruthar on 12/29/15.
 */
public class OrientDBClientTest {
  // TODO: This must be copied because it is private in OrientDBClient, but this should defer to table property.
  private static final String CLASS = "usertable";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX = "user";
  private static final int NUM_FIELDS = 3;
  private static final String TEST_DB_URL = "memory:test";

  private static ODictionary<ORecord> orientDBDictionary;
  private static OrientDBClient orientDBClient = null;

  @Before
  public void setup() throws DBException {
    orientDBClient = new OrientDBClient();

    Properties p = new Properties();
    // TODO: Extract the property names into final variables in OrientDBClient
    p.setProperty("orientdb.url", TEST_DB_URL);

    orientDBClient.setProperties(p);
    orientDBClient.init();
    orientDBDictionary = orientDBClient.getDB().getDictionary();
  }

  @After
  public void teardown() throws DBException {
    if (orientDBClient != null) {
      orientDBClient.cleanup();
    }
  }

  /*
      This is a copy of buildDeterministicValue() from core:com.yahoo.ycsb.workloads.CoreWorkload.java.
      That method is neither public nor static so we need a copy.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = FIELD_LENGTH;
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /*
      Inserts a row of deterministic values for the given insertKey using the orientDBClient.
   */
  private Map<String, ByteIterator> insertRow(String insertKey) {
    HashMap<String, ByteIterator> insertMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
    }
    orientDBClient.insert(CLASS, insertKey, insertMap);

    return insertMap;
  }

  @Test
  public void insertTest() {
    String insertKey = "user0";
    Map<String, ByteIterator> insertMap = insertRow(insertKey);

    ODocument result = orientDBDictionary.get(insertKey);

    assertTrue("Assert a row was inserted.", result != null);

    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals("Assert all inserted columns have correct values.", result.field(FIELD_PREFIX + i), insertMap.get(FIELD_PREFIX + i).toString());
    }
  }

  @Test
  public void updateTest() {
    String preupdateString = "preupdate";
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    // Manually insert three documents
    for(String key: Arrays.asList(user0, user1, user2)) {
      ODocument doc = new ODocument(CLASS);
      for (int i = 0; i < NUM_FIELDS; i++) {
        doc.field(FIELD_PREFIX + i, preupdateString);
      }
      doc.save();
      orientDBDictionary.put(key, doc);
    }

    HashMap<String, ByteIterator> updateMap = new HashMap<>();
    for (int i = 0; i < NUM_FIELDS; i++) {
      updateMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(user1, FIELD_PREFIX + i)));
    }

    orientDBClient.update(CLASS, user1, updateMap);

    // Ensure that user0 record was not changed
    ODocument result = orientDBDictionary.get(user0);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals("Assert first row fields contain preupdateString", result.field(FIELD_PREFIX + i), preupdateString);
    }

    // Check that all the columns have expected values for user1 record
    result = orientDBDictionary.get(user1);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals("Assert updated row fields are correct", result.field(FIELD_PREFIX + i), updateMap.get(FIELD_PREFIX + i).toString());
    }

    // Ensure that user2 record was not changed
    result = orientDBDictionary.get(user2);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals("Assert third row fields contain preupdateString", result.field(FIELD_PREFIX + i), preupdateString);
    }
  }

  @Test
  public void readTest() {
    String insertKey = "user0";
    Map<String, ByteIterator> insertMap = insertRow(insertKey);
    HashSet<String> readFields = new HashSet<>();
    HashMap<String, ByteIterator> readResultMap = new HashMap<>();

    // Test reading a single field
    readFields.add("FIELD0");
    orientDBClient.read(CLASS, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field: readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }

    readResultMap = new HashMap<>();

    // Test reading all fields
    readFields.add("FIELD1");
    readFields.add("FIELD2");
    orientDBClient.read(CLASS, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field: readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }
  }

  @Test
  public void deleteTest() {
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    insertRow(user0);
    insertRow(user1);
    insertRow(user2);

    orientDBClient.delete(CLASS, user1);

    assertNotNull("Assert user0 still exists", orientDBDictionary.get(user0));
    assertNull("Assert user1 does not exist", orientDBDictionary.get(user1));
    assertNotNull("Assert user2 still exists", orientDBDictionary.get(user2));
  }

  @Test
  public void scanTest() {
    Map<String, Map<String, ByteIterator>> keyMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }

    Set<String> fieldSet = new HashSet<>();
    fieldSet.add("FIELD0");
    fieldSet.add("FIELD1");
    int startIndex = 1;
    int resultRows = 3;

    Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    orientDBClient.scan(CLASS, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals("Assert the correct number of results rows were returned", resultRows, resultVector.size());

    /**
     * Part of the known issue about the broken iterator in orientdb is that the iterator
     * starts at index 1 instead of index 0. Because of this, to test it we must increment
     * the start index. When that known issue has been fixed, remove the increment below.
     * Track the issue here: https://github.com/orientechnologies/orientdb/issues/5541
     * This fix was implemented for orientechnologies:orientdb-client:2.1.8
     */
    int testIndex = startIndex;

    // Check each vector row to make sure we have the correct fields
    for (HashMap<String, ByteIterator> result: resultVector) {
      assertEquals("Assert that this row has the correct number of fields", fieldSet.size(), result.size());
      for (String field: fieldSet) {
        assertEquals("Assert this field is correct in this row", keyMap.get(KEY_PREFIX + testIndex).get(field).toString(), result.get(field).toString());
      }
      testIndex++;
    }
  }
}
