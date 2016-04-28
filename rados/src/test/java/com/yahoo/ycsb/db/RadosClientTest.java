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

import static org.testng.AssertJUnit.assertEquals;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
  private static final String KEY1 = "key0";
  private static final HashMap<String, ByteIterator> DATA;

  static {
    DATA = new HashMap<String, ByteIterator>(10);
    for (int i = 0; i < 10; i++) {
      DATA.put("key" + UUID.randomUUID(), new StringByteIterator("data" + UUID.randomUUID()));
    }
  }

  @BeforeClass
  public void setupClass() throws DBException {
    radosclient = new RadosClient();

    Properties p = new Properties();
    p.setProperty(POOL_PROPERTY, POOL_TEST);

    radosclient.setProperties(p);
    radosclient.init();
  }

  @AfterClass
  public void teardownClass() throws DBException {
    if (radosclient != null) {
      radosclient.cleanup();
    }
  }

  @BeforeMethod
  public void setUp() {
    radosclient.insert(TABLE_NAME, KEY0, DATA);
  }

  @AfterMethod
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
    HashMap<String, ByteIterator> newDATA = new HashMap<String, ByteIterator>(10);
    for (int i = 0; i < 10; i++) {
      newDATA.put("key" + UUID.randomUUID(), new StringByteIterator("new data" + UUID.randomUUID()));
    }

    Status result = radosclient.update(TABLE_NAME, KEY0, newDATA);
    assertEquals(Status.OK, result);

    HashMap<String, ByteIterator> ret = new HashMap<String, ByteIterator>(10);
    radosclient.read(TABLE_NAME, KEY0, DATA.keySet(), ret);
    compareMap(ret);
  }

  @Test
  public void readTest() {
    HashMap<String, ByteIterator> ret = new HashMap<String, ByteIterator>(10);
    Status result = radosclient.read(TABLE_NAME, KEY0, DATA.keySet(), ret);
    assertEquals(Status.OK, result);
    compareMap(ret);
  }

  private void compareMap(HashMap<String, ByteIterator> ret) {
    Set setDATA = DATA.entrySet();
    Set setReturn = ret.entrySet();
    Iterator itDATA = setDATA.iterator();
    Iterator itReturn = setReturn.iterator();
    for (int i = 0; i < 10; i++) {
      Map.Entry entryDATA = (Map.Entry)itDATA.next();
      Map.Entry entryReturn = (Map.Entry)itReturn.next();
      assertEquals(entryDATA.getKey(), entryReturn.getKey());
      assertEquals(entryDATA.getValue(), entryReturn.getValue());
    }
  }

  @Test
  public void deleteTest() {
    Status result = radosclient.delete(TABLE_NAME, KEY0);
    assertEquals(Status.OK, result);
  }

}
