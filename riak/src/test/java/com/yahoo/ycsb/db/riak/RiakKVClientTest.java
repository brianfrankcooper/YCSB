/**
 * Copyright (c) 2016 YCSB contributors All rights reserved.
 * Copyright 2014 Basho Technologies, Inc.
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

package com.yahoo.ycsb.db.riak;

import java.util.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

/**
 * Integration tests for the Riak KV client.
 */
public class RiakKVClientTest {
  private static RiakKVClient riakClient;

  private static final String bucket = "testBucket";
  private static final String keyPrefix = "testKey";
  private static final int recordsToInsert = 20;
  private static final int recordsToScan = 7;
  private static final String firstField = "Key number";
  private static final String secondField = "Key number doubled";
  private static final String thirdField = "Key number square";

  private static boolean testStarted = false;

  /**
   * Creates a cluster for testing purposes.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    riakClient = new RiakKVClient();
    riakClient.init();

    // Set the test bucket environment with the appropriate parameters.
    try {
      riakClient.setTestEnvironment(bucket);
    } catch(Exception e) {
      assumeNoException("Unable to configure Riak KV for test, aborting.", e);
    }

    // Just add some records to work on...
    for (int i = 0; i < recordsToInsert; i++) {
      // Abort the entire test whenever the dataset population operation fails.
      assumeThat("Riak KV is NOT RUNNING, aborting test.",
          riakClient.insert(bucket, keyPrefix + String.valueOf(i), StringByteIterator.getByteIteratorMap(
              createExpectedHashMap(i))),
              is(Status.OK));
    }

    // Variable to check to determine whether the test has started or not.
    testStarted = true;
  }

  /**
   * Shuts down the cluster created.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    // Delete all added keys before cleanup ONLY IF TEST ACTUALLY STARTED.
    if (testStarted) {
      for (int i = 0; i <= recordsToInsert; i++) {
        delete(keyPrefix + Integer.toString(i));
      }
    }

    riakClient.cleanup();
  }

  /**
   * Test method for read transaction. It is designed to read two of the three fields stored for each key, to also test
   * if the createResultHashMap() function implemented in RiakKVClient.java works as expected.
   */
  @Test
  public void testRead() {
    // Choose a random key to read, among the available ones.
    int readKeyNumber = new Random().nextInt(recordsToInsert);

    // Prepare two fields to read.
    Set<String> fields = new HashSet<>();
    fields.add(firstField);
    fields.add(thirdField);

    // Prepare an expected result.
    HashMap<String, String> expectedValue = new HashMap<>();
    expectedValue.put(firstField, Integer.toString(readKeyNumber));
    expectedValue.put(thirdField, Integer.toString(readKeyNumber * readKeyNumber));

    // Define a HashMap to store the actual result.
    HashMap<String, ByteIterator> readValue = new HashMap<>();

    // If a read transaction has been properly done, then one has to receive a Status.OK return from the read()
    // function. Moreover, the actual returned result MUST match the expected one.
    assertEquals("Read transaction FAILED.",
        Status.OK,
        riakClient.read(bucket, keyPrefix + Integer.toString(readKeyNumber), fields, readValue));

    assertEquals("Read test FAILED. Actual read transaction value is NOT MATCHING the expected one.",
        expectedValue.toString(),
        readValue.toString());
  }

  /**
   * Test method for scan transaction. A scan transaction has to be considered successfully completed only if all the
   * requested values are read (i.e. scan transaction returns with Status.OK). Moreover, one has to check if the
   * obtained results match the expected ones.
   */
  @Test
  public void testScan() {
    // Choose, among the available ones, a random key as starting point for the scan transaction.
    int startScanKeyNumber = new Random().nextInt(recordsToInsert - recordsToScan);

    // Prepare a HashMap vector to store the scan transaction results.
    Vector<HashMap<String, ByteIterator>> scannedValues = new Vector<>();

    // Check whether the scan transaction is correctly performed or not.
    assertEquals("Scan transaction FAILED.",
        Status.OK,
        riakClient.scan(bucket, keyPrefix + Integer.toString(startScanKeyNumber), recordsToScan, null,
            scannedValues));

    // After the scan transaction completes, compare the obtained results with the expected ones.
    for (int i = 0; i < recordsToScan; i++) {
      assertEquals("Scan test FAILED: the current scanned key is NOT MATCHING the expected one.",
          createExpectedHashMap(startScanKeyNumber + i).toString(),
          scannedValues.get(i).toString());
    }
  }

  /**
   * Test method for update transaction. The test is designed to restore the previously read key. It is assumed to be
   * correct when, after performing the update transaction, one reads the just provided values.
   */
  @Test
  public void testUpdate() {
    // Choose a random key to read, among the available ones.
    int updateKeyNumber = new Random().nextInt(recordsToInsert);

    // Define a HashMap to save the previously stored values for eventually restoring them.
    HashMap<String, ByteIterator> readValueBeforeUpdate = new HashMap<>();
    riakClient.read(bucket, keyPrefix + Integer.toString(updateKeyNumber), null, readValueBeforeUpdate);

    // Prepare an update HashMap to store.
    HashMap<String, String> updateValue = new HashMap<>();
    updateValue.put(firstField, "UPDATED");
    updateValue.put(secondField, "UPDATED");
    updateValue.put(thirdField, "UPDATED");

    // First of all, perform the update and check whether it's failed or not.
    assertEquals("Update transaction FAILED.",
        Status.OK,
        riakClient.update(bucket, keyPrefix + Integer.toString(updateKeyNumber), StringByteIterator
            .getByteIteratorMap(updateValue)));

    // Then, read the key again and...
    HashMap<String, ByteIterator> readValueAfterUpdate = new HashMap<>();
    assertEquals("Update test FAILED. Unable to read key value.",
        Status.OK,
        riakClient.read(bucket, keyPrefix + Integer.toString(updateKeyNumber), null, readValueAfterUpdate));

    // ...compare the result with the new one!
    assertEquals("Update transaction NOT EXECUTED PROPERLY. Values DID NOT CHANGE.",
        updateValue.toString(),
        readValueAfterUpdate.toString());

    // Finally, restore the previously read key.
    assertEquals("Update test FAILED. Unable to restore previous key value.",
        Status.OK,
        riakClient.update(bucket, keyPrefix + Integer.toString(updateKeyNumber), readValueBeforeUpdate));
  }

  /**
   * Test method for insert transaction. It is designed to insert a key just after the last key inserted in the setUp()
   * phase.
   */
  @Test
  public void testInsert() {
    // Define a HashMap to insert and another one for the comparison operation.
    HashMap<String, String> insertValue = createExpectedHashMap(recordsToInsert);
    HashMap<String, ByteIterator> readValue = new HashMap<>();

    // Check whether the insertion transaction was performed or not.
    assertEquals("Insert transaction FAILED.",
        Status.OK,
        riakClient.insert(bucket, keyPrefix + Integer.toString(recordsToInsert), StringByteIterator.
            getByteIteratorMap(insertValue)));

    // Finally, compare the insertion performed with the one expected by reading the key.
    assertEquals("Insert test FAILED. Unable to read inserted value.",
        Status.OK,
        riakClient.read(bucket, keyPrefix + Integer.toString(recordsToInsert), null, readValue));
    assertEquals("Insert test FAILED. Actual read transaction value is NOT MATCHING the inserted one.",
        insertValue.toString(),
        readValue.toString());
  }

  /**
   * Test method for delete transaction. The test deletes a key, then performs a read that should give a
   * Status.NOT_FOUND response. Finally, it restores the previously read key.
   */
  @Test
  public void testDelete() {
    // Choose a random key to delete, among the available ones.
    int deleteKeyNumber = new Random().nextInt(recordsToInsert);

    // Define a HashMap to save the previously stored values for its eventual restore.
    HashMap<String, ByteIterator> readValueBeforeDelete = new HashMap<>();
    riakClient.read(bucket, keyPrefix + Integer.toString(deleteKeyNumber), null, readValueBeforeDelete);

    // First of all, delete the key.
    assertEquals("Delete transaction FAILED.",
        Status.OK,
        delete(keyPrefix + Integer.toString(deleteKeyNumber)));

    // Then, check if the deletion was actually achieved.
    assertEquals("Delete test FAILED. Key NOT deleted.",
        Status.NOT_FOUND,
        riakClient.read(bucket, keyPrefix + Integer.toString(deleteKeyNumber), null, null));

    // Finally, restore the previously deleted key.
    assertEquals("Delete test FAILED. Unable to restore previous key value.",
        Status.OK,
        riakClient.insert(bucket, keyPrefix + Integer.toString(deleteKeyNumber), readValueBeforeDelete));
  }

  private static Status delete(String key) {
    return riakClient.delete(bucket, key);
  }

  private static HashMap<String, String> createExpectedHashMap(int value) {
    HashMap<String, String> values = new HashMap<>();

    values.put(firstField, Integer.toString(value));
    values.put(secondField, Integer.toString(2 * value));
    values.put(thirdField, Integer.toString(value * value));

    return values;
  }
}
