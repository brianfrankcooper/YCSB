/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNoException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.junit.AfterClass;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;


 /**
  * Test for the binding of <a href="http://ceph.org/">RADOS of Ceph</a>.
  *
  * See {@code rados/README.md} for details.
  */

public class RadosClientTest {

  private static RadosClient radosclient;

  public static final String POOL_PROPERTY = "rados.pool";
  public static final String POOL_TEST = "rbd";

  private static final String TABLE_NAME = "table0";
  private static final String KEY0 = "key0";
  private static final String KEY1 = "key1";
  private static final String KEY2 = "key2";
  private static final HashMap<String, ByteIterator> DATA;
  private static final HashMap<String, ByteIterator> DATA_UPDATED;

  static {
    DATA = new HashMap<String, ByteIterator>(10);
    DATA_UPDATED = new HashMap<String, ByteIterator>(10);
    for (int i = 0; i < 10; i++) {
      String key = "key" + UUID.randomUUID();
      DATA.put(key, new StringByteIterator("data" + UUID.randomUUID()));
      DATA_UPDATED.put(key, new StringByteIterator("data" + UUID.randomUUID()));
    }
  }

  @BeforeClass
  public static void setupClass() throws DBException {
    radosclient = new RadosClient();

    Properties p = new Properties();
    p.setProperty(POOL_PROPERTY, POOL_TEST);

    try {
      radosclient.setProperties(p);
      radosclient.init();
    }
    catch (DBException|UnsatisfiedLinkError e) {
      assumeNoException("Ceph cluster is not running. Skipping tests.", e);
    }
  }

  @AfterClass
  public static void teardownClass() throws DBException {
    if (radosclient != null) {
      radosclient.cleanup();
    }
  }

  @Before
  public void setUp() {
    radosclient.insert(TABLE_NAME, KEY0, DATA);
  }

  @After
  public void tearDown() {
    radosclient.delete(TABLE_NAME, KEY0);
  }

  @Test
  public void insertTest() {
    Status result = radosclient.insert(TABLE_NAME, KEY1, DATA);
    assertEquals(Status.OK, result);
  }

  @Test
  public void updateTest() {
    radosclient.insert(TABLE_NAME, KEY2, DATA);

    Status result = radosclient.update(TABLE_NAME, KEY2, DATA_UPDATED);
    assertEquals(Status.OK, result);

    HashMap<String, ByteIterator> ret = new HashMap<String, ByteIterator>(10);
    radosclient.read(TABLE_NAME, KEY2, DATA.keySet(), ret);
    compareMap(DATA_UPDATED, ret);

    radosclient.delete(TABLE_NAME, KEY2);
  }

  @Test
  public void readTest() {
    HashMap<String, ByteIterator> ret = new HashMap<String, ByteIterator>(10);
    Status result = radosclient.read(TABLE_NAME, KEY0, DATA.keySet(), ret);
    assertEquals(Status.OK, result);
    compareMap(DATA, ret);
  }

  private void compareMap(HashMap<String, ByteIterator> src, HashMap<String, ByteIterator> dest) {
    assertEquals(src.size(), dest.size());

    Set setSrc = src.entrySet();
    Iterator<Map.Entry> itSrc = setSrc.iterator();
    for (int i = 0; i < 10; i++) {
      Map.Entry<String, ByteIterator> entrySrc = itSrc.next();
      assertEquals(entrySrc.getValue().toString(), dest.get(entrySrc.getKey()).toString());
    }
  }

  @Test
  public void deleteTest() {
    Status result = radosclient.delete(TABLE_NAME, KEY0);
    assertEquals(Status.OK, result);
  }

}
