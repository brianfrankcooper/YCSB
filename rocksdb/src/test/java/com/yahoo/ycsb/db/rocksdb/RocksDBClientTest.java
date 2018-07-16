/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db.rocksdb;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksDBClientTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String MOCK_TABLE = "ycsb";
  private static final String MOCK_KEY0 = "0";
  private static final String MOCK_KEY1 = "1";
  private static final String MOCK_KEY2 = "2";
  private static final String MOCK_KEY3 = "3";
  private static final int NUM_RECORDS = 10;

  private static final Map<String, ByteIterator> MOCK_DATA;
  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put("field" + i, new StringByteIterator("value" + i));
    }
  }

  private RocksDBClient instance;

  @Before
  public void setup() throws Exception {
    instance = new RocksDBClient();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_DIR, tmpFolder.getRoot().getAbsolutePath());
    instance.setProperties(properties);

    instance.init();
  }

  @After
  public void tearDown() throws Exception {
    instance.cleanup();
  }

  @Test
  public void insertAndRead() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
    assertEquals(Status.OK, readResult);
  }

  @Test
  public void insertAndDelete() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
    assertEquals(Status.OK, result);
  }

  @Test
  public void insertUpdateAndRead() throws Exception {
    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put("field" + i, new StringByteIterator("newvalue" + i));
    }

    final Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
    }
  }

  @Test
  public void insertAndScan() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY3, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
    final Status result = instance.scan(MOCK_TABLE, MOCK_KEY3, NUM_RECORDS, fields, resultParam);
    assertEquals(Status.OK, result);
  }
}
