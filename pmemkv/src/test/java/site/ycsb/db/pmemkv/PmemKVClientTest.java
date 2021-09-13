/*
 * Copyright (c) 2018-2021 YCSB contributors. All rights reserved.
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

import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class PmemKVClientTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String MOCK_TABLE = "ycsb";
  private static final String MOCK_KEY0 = "0";
  private static final String MOCK_KEY1 = "1";
  private static final String MOCK_KEY2 = "2";
  private static final String MOCK_KEY3 = "3";
  private static final int NUM_RECORDS = 10;
  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  private static final Map<String, ByteIterator> MOCK_DATA;
  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
  }

  private PmemKVClient instance;

  @Before
  public void setup() throws Exception {
    instance = new PmemKVClient();

    final Properties properties = new Properties();
    properties.setProperty(PmemKVClient.PATH_PROPERTY, tmpFolder.getRoot().getAbsolutePath() + "/pmemkv_db");
    properties.setProperty(PmemKVClient.SIZE_PROPERTY, "60777216");
    instance.setProperties(properties);

    instance.init();
  }

  @After
  public void tearDown() throws Exception {
    instance.cleanup();
  }

  @Test
  public void dummyTest() throws Exception {
    assertEquals(Status.OK, Status.OK);
  }

  @Test
  public void readFromEmptyDB() throws Exception {
    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
    assertEquals(Status.NOT_FOUND, readResult);
  }

  @Test
  public void deleteNonExistingKey() throws Exception {
    final Status result = instance.delete(MOCK_TABLE, MOCK_KEY1+"_non_existent");
    assertEquals(Status.NOT_FOUND, result);
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

    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY1, fields, resultParam);
    assertEquals(Status.NOT_FOUND, readResult);
  }

  @Test
  public void insertUpdateAndRead() throws Exception {
    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    final Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    assertEquals(Status.OK, result);

    /* validate the values changed */
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
    }
  }

  @Test
  public void insertAndScan() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY3, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
    final Status result = instance.scan(MOCK_TABLE, MOCK_KEY3, NUM_RECORDS, fields, resultParam);
    assertEquals(Status.NOT_IMPLEMENTED, result);
  }
}
