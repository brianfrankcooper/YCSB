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
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
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
  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  private static final Map<String, ByteIterator> MOCK_DATA;

  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
  }

  private RocksDBClient instance;

  @Before
  public void setup() throws DBException {
    instance = new RocksDBClient();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_DIR, tmpFolder.getRoot().getAbsolutePath());
    instance.setProperties(properties);
  }

  @After
  public void tearDown() throws DBException {
    instance.cleanup();
  }

  @Test
  public void recoverFromExternalOptionsFile() throws DBException {
    instance.getProperties().setProperty(
        RocksDBClient.PROPERTY_OPTIONS_FILE,
        getClass().getClassLoader().getResource("OPTIONS").getFile());
    instance.init();

    // The column families should be able to be recovered by reading
    // from the external options file.
    assertEquals(instance.getColumnFamilies().keySet(),
        new HashSet<>(
            Arrays.asList(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8),
                "myusertable")));
  }

  @Test
  public void verifyRecoveryOfColumnFamilies() throws DBException {
    instance.init();

    // Initially it does not have the MOCK_TABLE column family
    assertEquals(
        new HashSet<>(Arrays.asList(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8))),
        instance.getColumnFamilies().keySet());

    // MOCK_TABLE should be created after the insert operation
    instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);

    instance.cleanup();

    setup();
    instance.init();

    // The MOCK_TABLE column family should be able to be automatially recovered
    // by reading from the options file in the dbPath.
    assertEquals(
        new HashSet<>(
            Arrays.asList(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8),
                MOCK_TABLE)),
        instance.getColumnFamilies().keySet());
  }

  @Test
  public void insertAndRead() throws DBException {
    instance.init();

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
    assertEquals(Status.OK, readResult);
  }

  @Test
  public void insertAndDelete() throws DBException {
    instance.init();

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
    assertEquals(Status.OK, result);
  }

  @Test
  public void insertUpdateAndRead() throws DBException {
    instance.init();

    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    final Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
    }
  }

  @Test
  public void insertAndScan() throws DBException {
    instance.init();

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY3, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
    final Status result = instance.scan(MOCK_TABLE, MOCK_KEY3, NUM_RECORDS, fields, resultParam);
    assertEquals(Status.OK, result);
  }
}
