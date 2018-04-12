/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.

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
package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.ignite.IgniteClient;
import com.yahoo.ycsb.db.ignite.IgniteYCSBClient;
import org.apache.ignite.Ignition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class IgniteYCSBClientTest {
  
  static IgniteYCSBClient igniteClient;
  private static String tableName;
  public final String testKey = "a";

  @BeforeClass
  public static void setup() throws DBException {
    IgniteClient.startIgnite("127.0.0.1", false);
    igniteClient = new IgniteYCSBClient();
    igniteClient.init();
    tableName = igniteClient.getTableName();

  }

  @AfterClass
  public static void tearDown() {
    Ignition.stopAll(true);
  }


  @Test
  public void testInsert() {
    Map<String, String> testData = createTestData();
    Status res = igniteClient.insert(tableName, testKey, StringByteIterator.getByteIteratorMap(testData));
    assertEquals(Status.OK, res);
  }

  @Test
    public void testUpdate() {
      Map<String, String> testData = createTestData();
      Status res = igniteClient.update(tableName, testKey, StringByteIterator.getByteIteratorMap(testData));
      assertEquals(Status.OK, res);
    }

  @Test
  public void testRead() {
    Map<String, String> testData = createTestData();
    Status res = igniteClient.insert(tableName, testKey, StringByteIterator.getByteIteratorMap(testData));
    assertEquals(Status.OK, res);

    Map<String, ByteIterator> results = new HashMap();
    res = igniteClient.read( tableName, testKey, null, results );
    assertEquals(Status.OK, res);
    assertEquals("aaaa", results.get(testKey).toString());
  }

  private Map<String, String> createTestData() {
    Map res = new HashMap();
    res.put(testKey, "aaaa");
    return res;
  }
  

}
