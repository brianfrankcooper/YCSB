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

package site.ycsb.db;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.StringByteIterator;

import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNoException;

/**
 * Created by kruthar on 12/29/15.
 */
@Ignore
public class OrientDBV2xClientTest {
  private static final String CLASS        = "usertable";
  private static final int    FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX   = "user";
  private static final int    NUM_FIELDS   = 3;
  private static final String TEST_DB_URL  = "memory:test";

  private static OrientDBV2xClient orientDBClient = null;

  @Before
  public void setup() {
    orientDBClient = new OrientDBV2xClient();

    Properties p = new Properties();
    // TODO: Extract the property names into final variables in OrientDBClient
    p.setProperty("orientdb.url", TEST_DB_URL);

    orientDBClient.setProperties(p);
    try {
      orientDBClient.init();
    } catch (DBException e) {
      try {
        orientDBClient.cleanup();
        assumeNoException("PostgreSQL is not running. Skipping tests.", e);
      } catch (DBException ignored) {
      }
    }
  }

  @After
  public void teardown() throws DBException {
    if (orientDBClient != null) {
      orientDBClient.cleanup();
    }
  }

  /*
      This is a copy of buildDeterministicValue() from core:site.ycsb.workloads.CoreWorkload.java.
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

    final OPartitionedDatabasePool pool = orientDBClient.getDatabasePool();
    try (final ODatabaseDocumentTx session = pool.acquire()) {
      ODictionary<ORecord> dictionary = session.getDictionary();
      ODocument result = dictionary.get(insertKey);
      assertTrue("Assert a row was inserted.", result != null);

      for (int i = 0; i < NUM_FIELDS; i++) {
        assertEquals("Assert all inserted columns have correct values.", result.field(FIELD_PREFIX + i),
            insertMap.get(FIELD_PREFIX + i).toString());
      }
    }
  }

  @Test
  public void updateTest() {
    String preupdateString = "preupdate";
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    OPartitionedDatabasePool pool = orientDBClient.getDatabasePool();
    try (final ODatabaseDocumentTx session = pool.acquire()) {
      // Manually insert three documents
      for (String key : Arrays.asList(user0, user1, user2)) {
        ODocument doc = new ODocument(CLASS);
        for (int i = 0; i < NUM_FIELDS; i++) {
          doc.field(FIELD_PREFIX + i, preupdateString);
        }
        doc.save();

        ODictionary<ORecord> dictionary = session.getDictionary();
        dictionary.put(key, doc);
      }
    }

    HashMap<String, ByteIterator> updateMap = new HashMap<>();
    for (int i = 0; i < NUM_FIELDS; i++) {
      updateMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(user1, FIELD_PREFIX + i)));
    }

    orientDBClient.update(CLASS, user1, updateMap);

    try (final ODatabaseDocumentTx session = pool.acquire()) {
      ODictionary<ORecord> dictionary = session.getDictionary();
      // Ensure that user0 record was not changed
      ODocument result = dictionary.get(user0);
      for (int i = 0; i < NUM_FIELDS; i++) {
        assertEquals("Assert first row fields contain preupdateString", result.field(FIELD_PREFIX + i), preupdateString);
      }

      // Check that all the columns have expected values for user1 record
      result = dictionary.get(user1);
      for (int i = 0; i < NUM_FIELDS; i++) {
        assertEquals("Assert updated row fields are correct", result.field(FIELD_PREFIX + i),
            updateMap.get(FIELD_PREFIX + i).toString());
      }

      // Ensure that user2 record was not changed
      result = dictionary.get(user2);
      for (int i = 0; i < NUM_FIELDS; i++) {
        assertEquals("Assert third row fields contain preupdateString", result.field(FIELD_PREFIX + i), preupdateString);
      }
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
    for (String field : readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }
    readResultMap = new HashMap<>();

    // Test reading all fields
    readFields.add("FIELD1");
    readFields.add("FIELD2");
    orientDBClient.read(CLASS, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field : readFields) {
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

    OPartitionedDatabasePool pool = orientDBClient.getDatabasePool();
    try (final ODatabaseDocumentTx session = pool.acquire()) {
      ODictionary<ORecord> dictionary = session.getDictionary();

      assertNotNull("Assert user0 still exists", dictionary.get(user0));
      assertNull("Assert user1 does not exist", dictionary.get(user1));
      assertNotNull("Assert user2 still exists", dictionary.get(user2));
    }
  }

  @Test
  public void scanTest() {
    final Map<String, Map<String, ByteIterator>> keyMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }

    final Set<String> fieldSet = new HashSet<>();
    fieldSet.add("FIELD0");
    fieldSet.add("FIELD1");
    int startIndex = 0;
    int resultRows = 3;

    final Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    orientDBClient.scan(CLASS, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals("Assert the correct number of results rows were returned", resultRows, resultVector.size());

    int testIndex = startIndex;

    // Check each vector row to make sure we have the correct fields
    for (HashMap<String, ByteIterator> result : resultVector) {
      assertEquals("Assert that this row has the correct number of fields", fieldSet.size(), result.size());
      for (String field : fieldSet) {
        assertEquals("Assert this field is correct in this row", keyMap.get(KEY_PREFIX + testIndex).get(field).toString(),
            result.get(field).toString());
      }
      testIndex++;
    }
  }
}
