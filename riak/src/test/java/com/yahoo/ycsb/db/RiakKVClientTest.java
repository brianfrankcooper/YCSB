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

package com.yahoo.ycsb.db;

import java.util.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for the Riak KV client. All tests are configured to return positive.
 */
public class RiakKVClientTest {
  private static RiakKVClient riakClient;
  private static String bucket = "testBucket";
  private static String startKey = "testKey";
  private static int recordsToInitiallyInsert = 10;

  /**
   * Creates a cluster for testing purposes.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    riakClient = new RiakKVClient();
    riakClient.init();

    HashMap<String, String> values = new HashMap<>();

    // Just add some random values to work on...
    for (int i = 0; i < recordsToInitiallyInsert; i++) {
      values.put("testRecord_1", "testValue_1");
      values.put("testRecord_2", "testValue_2");

      riakClient.insert(bucket, startKey + String.valueOf(i), StringByteIterator.getByteIteratorMap(values));
    }
  }

  /**
   * Shuts down the cluster created.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    riakClient.cleanup();
  }

  /**
   * Test method for read transaction.
   */
  @Test
  public void testRead() {
    Set<String> fields = new HashSet<>();
    fields.add("testRecord_1");
    fields.add("testRecord_2");

    HashMap<String, ByteIterator> results = new HashMap<>();

    assertEquals(Status.OK, riakClient.read(bucket, startKey + "1", fields, results));
  }

  /**
   * Test method for scan transaction.
   */
  @Test
  public void testScan() {
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();

    assertEquals(Status.OK, riakClient.scan(bucket, startKey + "1", recordsToInitiallyInsert - 1, null, results));
  }

  /**
   * Test method for update transaction.
   */
  @Test
  public void testUpdate() {
    HashMap<String, String> values = new HashMap<>();
    values.put("testRecord_1", "testValue_1_updated");
    values.put("testRecord_2", "testValue_2_updated");

    assertEquals(Status.OK, riakClient.update(bucket, startKey + "0", StringByteIterator.getByteIteratorMap(values)));
  }

  /**
   * Test method for insert transaction.
   */
  @Test
  public void testInsert() {
    HashMap<String, String> values = new HashMap<>();
    values.put("testRecord_1", "testValue_1");
    values.put("testRecord_2", "testValue_2");

    assertEquals(Status.OK, riakClient.insert(bucket, startKey + Integer.toString(recordsToInitiallyInsert),
        StringByteIterator.getByteIteratorMap(values)));
  }

  /**
   * Test method for delete transaction.
   */
  @Test
  public void testDelete() {
    assertEquals(Status.OK, riakClient.delete(bucket, startKey + "0"));
  }
}
