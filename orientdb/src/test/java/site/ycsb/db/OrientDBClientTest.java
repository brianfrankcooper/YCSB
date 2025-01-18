/**
 * Copyright (c) 2012 - 2021 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class OrientDBClientTest {
  private static final String CLASS = "usertable";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX = "user";
  private static final int NUM_FIELDS = 3;
  private static final String TEST_DB_URL = "memory:test"; // "remote:localhost/test"; 

  private static OrientDBClient OrientDBClient = null;

  @Before
  public void setup() throws DBException {
    OrientDBClient = new OrientDBClient();

    final Properties p = new Properties();
    // TODO: Extract the property names into final variables in OrientDBClient
    p.setProperty("orientdb.url", TEST_DB_URL);

    OrientDBClient.setProperties(p);
    OrientDBClient.init();
  }

  @After
  public void teardown() throws DBException {
    if (OrientDBClient != null) {
      OrientDBClient.dropTable("test");
      OrientDBClient.cleanup();
    }
  }

  @Test
  public void insertTest() {
    final String insertKey = "user0-insert";
    final Map<String, ByteIterator> insertMap = insertRow(insertKey);
    Assert.assertEquals(Status.OK, OrientDBClient.insert(CLASS, insertKey, insertMap));
  }

  @Test
  public void readTest() {
    final String insertKey = "user0";
    final Map<String, ByteIterator> insertMap = insertRow(insertKey);
    final HashSet<String> readFields = new HashSet<>();

    // Test reading a single field
    readFields.add("FIELD0");
    Map<String, ByteIterator> readResultMap = new HashMap<>();
    OrientDBClient.read(CLASS, insertKey, readFields, readResultMap);
    assertEquals(
        "Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (final String field : readFields) {
      assertEquals(
          "Assert " + field + " was read correctly",
          insertMap.get(field).toString(),
          readResultMap.get(field).toString());
    }
    readResultMap = new HashMap<>();

    // Test reading two fields
    readFields.add("FIELD1");
    readFields.add("FIELD2");
    OrientDBClient.read(CLASS, insertKey, readFields, readResultMap);
    assertEquals(
        "Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (final String field : readFields) {
      assertEquals(
          "Assert " + field + " was read correctly",
          insertMap.get(field).toString(),
          readResultMap.get(field).toString());
    }
  }

  @Test
  public void updateTest() {
    String preupdateString = "preupdate";
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    ODatabasePool pool = OrientDBClient.getDatabasePool();
    try (final ODatabaseSession session = pool.acquire()) {
      session.begin();
      // Manually insert three documents
      for (final String key : Arrays.asList(user0, user1, user2)) {
        ODocument doc = new ODocument(CLASS);
        for (int i = 0; i < NUM_FIELDS; i++) {
          doc.field("key", key);
          doc.field(FIELD_PREFIX + i, preupdateString);
        }
        doc.save();
        // ODictionary<ORecord> dictionary = session.getDictionary();
        // dictionary.put(key, doc);
      }
      session.commit();
    }

    final Map<String, ByteIterator> updateMap = new HashMap<>();
    for (int i = 0; i < NUM_FIELDS; i++) {
      updateMap.put(
          FIELD_PREFIX + i,
          new StringByteIterator(buildDeterministicValue(user1, FIELD_PREFIX + i)));
    }
    OrientDBClient.update(CLASS, user1, updateMap);

    Map<String, ByteIterator> readResultMap = new HashMap<>();
    OrientDBClient.read(CLASS, user0, null, readResultMap);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals(
          "Assert first row fields contain preupdateString",
          readResultMap.get(FIELD_PREFIX + i).toString(),
          preupdateString);
    }
    readResultMap.clear();

    // Check that all the columns have expected values for user1 record
    OrientDBClient.read(CLASS, user1, null, readResultMap);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals(
          "Assert updated row fields are correct",
          readResultMap.get(FIELD_PREFIX + i).toString(),
          updateMap.get(FIELD_PREFIX + i).toString());
    }
    readResultMap.clear();

    // Ensure that user2 record was not changed
    OrientDBClient.read(CLASS, user0, null, readResultMap);
    for (int i = 0; i < NUM_FIELDS; i++) {
      assertEquals(
          "Assert third row fields contain preupdateString",
          readResultMap.get(FIELD_PREFIX + i).toString(),
          preupdateString);
    }
  }

  @Test
  public void deleteTest() {
    final String user0 = "user0";
    final String user1 = "user1";
    final String user2 = "user2";

    insertRow(user0);
    insertRow(user1);
    insertRow(user2);
    OrientDBClient.delete(CLASS, user1);

    final Map<String, ByteIterator> readResultMap = new HashMap<>();
    OrientDBClient.read(CLASS, user0, null, readResultMap);
    assertEquals("Assert user0 still exists", "user0", readResultMap.get("key").toString());
    readResultMap.clear();
    OrientDBClient.read(CLASS, user1, null, readResultMap);
    assertEquals("Assert user1 does not exist", 0, readResultMap.size());
    readResultMap.clear();
    OrientDBClient.read(CLASS, user2, null, readResultMap);
    assertEquals("Assert user2 still exists", "user2", readResultMap.get("key").toString());
    readResultMap.clear();
  }

  @Test
  public void scanTest() {
    final Map<String, Map<String, ByteIterator>> keyMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      final String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }
    final Set<String> fieldSet = new HashSet<>();
    fieldSet.add("FIELD0");
    fieldSet.add("FIELD1");
    int startIndex = 0;
    int resultRows = 3;

    final Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    OrientDBClient.scan(CLASS, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals(
        "Assert the correct number of results rows were returned", resultRows, resultVector.size());

    int testIndex = startIndex;

    // Check each vector row to make sure we have the correct fields
    for (final Map<String, ByteIterator> result : resultVector) {
      assertEquals(
          "Assert that this row has the correct number of fields", fieldSet.size(), result.size());
      for (String field : fieldSet) {
        assertEquals(
            "Assert this field is correct in this row",
            keyMap.get(KEY_PREFIX + testIndex).get(field).toString(),
            result.get(field).toString());
      }
      testIndex++;
    }
  }

  @Test
  public void scanTestFieldsNull() {
    final Map<String, Map<String, ByteIterator>> keyMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      final String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }
    Set<String> fieldSet = null;
    int startIndex = 0;
    int resultRows = 3;

    final Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    OrientDBClient.scan(CLASS, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals(
        "Assert the correct number of results rows were returned", resultRows, resultVector.size());

    for (final Map<String, ByteIterator> result : resultVector) {
      Assert.assertNotNull("Assert that each result has a key", result.get("key"));
    }
  }

  /*
     This is a copy of buildDeterministicValue() from core:site.ycsb.workloads.CoreWorkload.java.
     That method is neither public nor static so we need a copy.
  */
  private String buildDeterministicValue(final String key, final String fieldkey) {
    int size = FIELD_LENGTH;
    final StringBuilder sb = new StringBuilder(size);
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
     Inserts a row of deterministic values for the given insertKey using the OrientDBClient.
  */
  private Map<String, ByteIterator> insertRow(final String insertKey) {
    HashMap<String, ByteIterator> insertMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      insertMap.put(
          FIELD_PREFIX + i,
          new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
    }
    OrientDBClient.insert(CLASS, insertKey, insertMap);
    return insertMap;
  }
}
