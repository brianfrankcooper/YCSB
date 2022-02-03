/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.voltdb.test;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNoException;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.voltdb.ConnectionHelper;
import site.ycsb.db.voltdb.VoltClient4;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.Properties;

/**
 * Test harness for YCSB / VoltDB. Note that not much happens if VoltDB isn't
 * visible.
 * 
 */
public class VoltDBClientTest {

  private static final String TABLE_NAME = "USERTABLE";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final int NUM_FIELDS = 3;

  private static final String INSERT_TEST_KEY = "InsertReadTest";
  private static final String INSERT_DELETE_AND_READ_TEST_KEY = "InsertDeleteReadTest";
  private static final String UPDATE_TEST_KEY = "UpdateTest";
  private static final String NON_EXISTENT_KEY = "NonExistTest";

  private static final String SCAN_KEY_PREFIX = "ScanKey_";
  private static final int SCAN_RECORD_COUNT = 5000;

  private static final String[] TEST_DATA_KEYS = { INSERT_TEST_KEY, INSERT_DELETE_AND_READ_TEST_KEY, UPDATE_TEST_KEY };

  private static VoltClient4 voltClient = null;
  private static boolean haveDb = false;

  @BeforeClass
  public static void setup() {

    Properties p = new Properties();

    String servers = p.getProperty("voltdb.servers", "localhost");
    String user = p.getProperty("voltdb.user", "");
    String password = p.getProperty("voltdb.password", "");
    String strLimit = p.getProperty("voltdb.ratelimit", "70000");

    p.setProperty("voltdb.servers", servers);
    p.setProperty("voltdb.user", user);
    p.setProperty("voltdb.password", password);
    p.setProperty("voltdb.ratelimit", strLimit);

    try {
      voltClient = new VoltClient4();
      voltClient.setProperties(p);

      if (ConnectionHelper.checkDBServers(servers)) {
        voltClient.init();
        haveDb = true;
        removeExistingData();
      }

    } catch (Exception e) {
      // The call to checkDBServers above looks for activity on
      // the ip and port we expect VoltDB to be on. If we get to this
      // line it's because 'something' is running on localhost:21212,
      // but whatever it is, it isn't a happy copy of VoltDB.
      assumeNoException("Something was running on VoltDB's port but it wasn't a usable copy of VoltDB", e);
    }
    
  }

  private static void removeExistingData() {

    try {
      for (int i = 0; i < TEST_DATA_KEYS.length; i++) {
        voltClient.delete(TABLE_NAME, TEST_DATA_KEYS[i]);
      }

      for (int i = 0; i < SCAN_RECORD_COUNT; i++) {
        voltClient.delete(TABLE_NAME, SCAN_KEY_PREFIX + i);
      }

    } catch (Exception e) {
      Logger logger = LoggerFactory.getLogger(VoltDBClientTest.class);
      logger.error("Error while calling 'removeExistingData()'", e);
      fail("Failed removeExistingData");
    }
  }

  @AfterClass
  public static void teardown() {
    
    try {
      if (voltClient != null && haveDb) {
        removeExistingData();
        voltClient.cleanup();
      }
    } catch (DBException e) {
      e.printStackTrace();
    }

  }

  @Before
  public void prepareTest() {
  }

  private boolean compareContents(HashMap<String, ByteIterator> inMsg, Map<String, ByteIterator> outMsg) {

    if (inMsg == null) {
      return false;
    }

    if (outMsg == null) {
      return false;
    }

    if (inMsg.size() != outMsg.size()) {
      return false;
    }

    @SuppressWarnings("rawtypes")
    Iterator it = inMsg.entrySet().iterator();
    while (it.hasNext()) {
      @SuppressWarnings("rawtypes")
      Map.Entry pair = (Map.Entry) it.next();
      String key = (String) pair.getKey();
      ByteIterator inPayload = inMsg.get(key);
      inPayload.reset();
      ByteIterator outPayload = outMsg.get(key);
      outPayload.reset();

      if (inPayload.bytesLeft() != outPayload.bytesLeft()) {
        return false;
      }

      while (inPayload.hasNext()) {
        byte inByte = inPayload.nextByte();
        byte outByte = outPayload.nextByte();
        if (inByte != outByte) {
          return false;
        }
      }

      it.remove();
    }

    return true;

  }

  @Test
  public void insertAndReadTest() {
    
    Assume.assumeTrue(haveDb);

    try {

      // Create some test data
      final String insertKey = INSERT_TEST_KEY;
      final Set<String> columns = getColumnNameMap();

      // Insert row
      HashMap<String, ByteIterator> insertMap = new HashMap<String, ByteIterator>();
      for (int i = 0; i < NUM_FIELDS; i++) {
        insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
      }
      voltClient.insert(TABLE_NAME, insertKey, insertMap);

      // Create a object to put retrieved row in...
      Map<String, ByteIterator> testResult = new HashMap<String, ByteIterator>();

      // Read row...
      Status s = voltClient.read(TABLE_NAME, insertKey, columns, testResult);

      if (!s.equals(Status.OK)) {
        fail("Didn't get OK on read.");
      }

      if (!compareContents(insertMap, testResult)) {
        fail("Returned data not the same as inserted data");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed insertTest");
    }
  }

  @Test
  public void insertDeleteAndReadTest() {
    
    Assume.assumeTrue(haveDb);

    try {

      // Create some test data
      final String insertKey = INSERT_DELETE_AND_READ_TEST_KEY;
      final Set<String> columns = getColumnNameMap();

      // Insert row
      HashMap<String, ByteIterator> insertMap = new HashMap<String, ByteIterator>();
      for (int i = 0; i < NUM_FIELDS; i++) {
        insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
      }
      voltClient.insert(TABLE_NAME, insertKey, insertMap);

      // Create a object to put retrieved row in...
      Map<String, ByteIterator> testResult = new HashMap<String, ByteIterator>();

      // Read row...
      Status s = voltClient.read(TABLE_NAME, insertKey, columns, testResult);

      if (!s.equals(Status.OK)) {
        fail("Didn't get OK on read.");
      }

      if (!compareContents(insertMap, testResult)) {
        fail("Returned data not the same as inserted data");
      }

      voltClient.delete(TABLE_NAME, insertKey);

      // Create another object to put retrieved row in...
      Map<String, ByteIterator> testResultAfterDelete = new HashMap<String, ByteIterator>();

      // Read row...
      voltClient.read(TABLE_NAME, insertKey, columns, testResultAfterDelete);

      if (testResultAfterDelete.size() > 0) {
        fail("testResultAfterDelete has value.");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed insertDeleteAndReadTest");
    }
  }

  @Test
  public void deleteNonExistentRecordTest() {
    
    Assume.assumeTrue(haveDb);

    try {

      // Create some test data
      final String insertKey = NON_EXISTENT_KEY;
      final Set<String> columns = getColumnNameMap();

      // Create a object to put retrieved row in...
      Map<String, ByteIterator> testResult = new HashMap<String, ByteIterator>();

      // Read row...
      voltClient.read(TABLE_NAME, insertKey, columns, testResult);

      if (testResult.size() > 0) {
        fail("testResult.size() > 0.");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed deleteNonExistentRecordTest");
    }
  }

  @Test
  public void scanReadTest() {

    Assume.assumeTrue(haveDb);
    
    try {

      for (int z = 0; z < SCAN_RECORD_COUNT; z++) {
        // Create some test data
        final String insertKey = SCAN_KEY_PREFIX + z;

        // Insert row
        HashMap<String, ByteIterator> insertMap = new HashMap<String, ByteIterator>();
        for (int i = 0; i < NUM_FIELDS; i++) {
          insertMap.put(FIELD_PREFIX + i, new StringByteIterator("Data for " + SCAN_KEY_PREFIX + z + " element " + i));
        }
        voltClient.insert(TABLE_NAME, insertKey, insertMap);
      }

      final String firstInsertKey = SCAN_KEY_PREFIX + 0;
      final String lastInsertKey = SCAN_KEY_PREFIX + (SCAN_RECORD_COUNT - 1);
      final String beyondLastInsertKey = SCAN_KEY_PREFIX + (SCAN_RECORD_COUNT + 1);
      final String oneHundredFromEndInsertKey = SCAN_KEY_PREFIX + (SCAN_RECORD_COUNT - 101);
      final String fiftyFromEndInsertKey = SCAN_KEY_PREFIX + (SCAN_RECORD_COUNT - 101);

      // test non existent records
      singleScanReadTest(NON_EXISTENT_KEY, 1000, 0, NON_EXISTENT_KEY);

      // test single record
      singleScanReadTest(firstInsertKey, 1, 1, firstInsertKey);

      // test scan of SCAN_RECORD_COUNT records
      singleScanReadTest(firstInsertKey, SCAN_RECORD_COUNT, SCAN_RECORD_COUNT, lastInsertKey);

      // test single record in middle
      singleScanReadTest(oneHundredFromEndInsertKey, 1, 1, oneHundredFromEndInsertKey);

      // test request of 100 starting 50 from end.
      singleScanReadTest(fiftyFromEndInsertKey, 100, 50, lastInsertKey);

      // test request of 100 starting beyond the end
      singleScanReadTest(beyondLastInsertKey, 100, 0, lastInsertKey);

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed scanReadTest");
    }

  }

  private void singleScanReadTest(String startKey, int requestedCount, int expectedCount, String lastKey) {
    
    Assume.assumeTrue(haveDb);

    try {

      final Set<String> columns = getColumnNameMap();

      // Create a object to put retrieved row in...
      Vector<HashMap<String, ByteIterator>> testResult = new Vector<HashMap<String, ByteIterator>>();

      // Read row...
      Status s = voltClient.scan(TABLE_NAME, startKey, expectedCount, columns, testResult);

      if (!s.equals(Status.OK)) {
        fail("Didn't get OK on read.");
      }

      if (testResult.size() != expectedCount) {
        fail("Failed singleScanReadTest " + startKey + " " + expectedCount + " " + lastKey);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed singleScanReadTest " + startKey + ". Asked for " + requestedCount + ", expected " + expectedCount
          + " lastkey=" + lastKey);
    }
  }

  @Test
  public void updateTest() {
    
    Assume.assumeTrue(haveDb);

    try {

      // Create some test data
      final String insertKey = UPDATE_TEST_KEY;

      // Insert row
      // Insert row
      HashMap<String, ByteIterator> insertThenUpdateMap = new HashMap<String, ByteIterator>();
      for (int i = 0; i < NUM_FIELDS; i++) {
        insertThenUpdateMap.put(FIELD_PREFIX + i,
            new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
      }
      voltClient.insert(TABLE_NAME, insertKey, insertThenUpdateMap);

      // Change the data we inserted...
      for (int i = 0; i < NUM_FIELDS; i++) {
        insertThenUpdateMap.put(FIELD_PREFIX + i, new StringByteIterator(FIELD_PREFIX + i + " has changed"));
      }

      // now do an update
      voltClient.update(TABLE_NAME, insertKey, insertThenUpdateMap);

      // Create a object to put retrieved row in...
      final Set<String> columns = getColumnNameMap();
      Map<String, ByteIterator> testResult = new HashMap<String, ByteIterator>();

      // Read row...
      Status s = voltClient.read(TABLE_NAME, insertKey, columns, testResult);

      if (!s.equals(Status.OK)) {
        fail("Didn't get OK on read.");
      }

      if (!compareContents(insertThenUpdateMap, testResult)) {
        fail("Returned data not the same as inserted data");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed updateTest");
    }
  }

  /**
   * @return
   */
  private Set<String> getColumnNameMap() {
    Set<String> columns = new HashSet<String>();
    for (int i = 0; i < NUM_FIELDS; i++) {
      columns.add(FIELD_PREFIX + i);
    }
    return columns;
  }

  /*
   * This is a copy of buildDeterministicValue() from
   * core:site.ycsb.workloads.CoreWorkload.java. That method is neither
   * public nor static so we need a copy.
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

}
