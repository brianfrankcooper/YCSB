/**
* Copyright (c) 2020 YCSB contributors. All rights reserved.
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

package site.ycsb.db.zookeeper;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * Integration tests for the YCSB ZooKeeper client.
 */
public class ZKClientTest {

  private static TestingServer zkTestServer;
  private ZKClient client;
  private String tableName;
  private final static String path = "benchmark";
  private static final int PORT = 2181;

  @BeforeClass
  public static void setUpClass() throws Exception {
    zkTestServer = new TestingServer(PORT);
    zkTestServer.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    zkTestServer.stop();
  }

  @Before
  public void setUp() throws Exception {
    client = new ZKClient();

    Properties p = new Properties();
    p.setProperty("zookeeper.connectString", "127.0.0.1:" + String.valueOf(PORT));

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDown() throws Exception {
    client.cleanup();
  }

  @Test
  public void testZKClient() {
    // insert
    Map<String, String> m = new HashMap<>();
    String field1 = "field_1";
    String value1 = "value_1";
    m.put(field1, value1);
    Map<String, ByteIterator> result = StringByteIterator.getByteIteratorMap(m);
    client.insert(tableName, path, result);

    // read
    result.clear();
    Status status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    assertEquals(1, result.size());
    assertEquals(value1, result.get(field1).toString());

    // update(the same field)
    m.clear();
    result.clear();
    String newVal = "value_new";
    m.put(field1, newVal);
    result = StringByteIterator.getByteIteratorMap(m);
    client.update(tableName, path, result);
    assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    // here we only have one field: field_1
    assertEquals(1, result.size());
    assertEquals(newVal, result.get(field1).toString());

    // update(two different field)
    m.clear();
    result.clear();
    String field2 = "field_2";
    String value2 = "value_2";
    m.put(field2, value2);
    result = StringByteIterator.getByteIteratorMap(m);
    client.update(tableName, path, result);
    assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    assertEquals(Status.OK, status);
    // here we have two field: field_1 and field_2
    assertEquals(2, result.size());
    assertEquals(value2, result.get(field2).toString());
    assertEquals(newVal, result.get(field1).toString());

    // delete
    status = client.delete(tableName, path);
    assertEquals(Status.OK, status);

    // Verify result
    result.clear();
    status = client.read(tableName, path, null, result);
    // NoNode return ERROR
    assertEquals(Status.ERROR, status);
    assertEquals(0, result.size());
  }

  @Test
  @Ignore("Not yet implemented")
  public void testScan() {
    fail("Not yet implemented");
  }
}

