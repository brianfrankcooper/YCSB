/**
 * Copyright (c) 2023 YCSB contributors. All rights reserved.
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

package site.ycsb.db.etcd;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import static junit.framework.TestCase.assertEquals;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * Integration tests for the YCSB etcd client.
 */
public class EtcdClientTest {
  private static TestingServer etcdTestServer;
  private EtcdClient client;
  private String tableName;
  private static final int PORT = 2379;

  @BeforeClass
  public static void setUpClass() throws Exception {
    etcdTestServer = new TestingServer(PORT);
    etcdTestServer.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    etcdTestServer.stop();
  }

  @Before
  public void setUp() throws Exception {
    client = new EtcdClient();

    Properties p = new Properties();
    p.setProperty("etcd.connectString", "http://127.0.0.1:" + PORT);

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
  public void testEtcdClient() {
    // insert
    Map<String, String> m = new HashMap<>();
    String field1 = "field_1";
    String value1 = "value_1";
    m.put(field1, value1);
    Map<String, ByteIterator> result = StringByteIterator.getByteIteratorMap(m);
    client.insert(tableName, field1, result);

    // read
    result.clear();
    Status status = client.read(tableName, field1, null, result);
    assertEquals(Status.OK, status);
    assertEquals(1, result.size());
    assertEquals(value1, result.get(field1).toString());

    // update(the same field)
    m.clear();
    result.clear();
    String newVal = "value_new";
    m.put(field1, newVal);
    result = StringByteIterator.getByteIteratorMap(m);
    client.update(tableName, field1, result);
    assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, field1, null, result);
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
    client.update(tableName, field2, result);
    assertEquals(1, result.size());

    // Verify result
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    String fieldPrefix = "field";
    status = client.scan(tableName, fieldPrefix, 0, null, results);
    assertEquals(Status.OK, status);
    // here we have two field: field_1 and field_2
    assertEquals(2, results.size());

    // delete
    status = client.delete(tableName, field1);
    assertEquals(Status.OK, status);
    status = client.delete(tableName, field2);
    assertEquals(Status.OK, status);

    // Verify result
    results.clear();
    status = client.scan(tableName, fieldPrefix, 2, null, results);
    // NoNode return ERROR
    assertEquals(Status.NOT_FOUND, status);
    assertEquals(0, results.size());
  }

  @Test
  public void testScan() {
    String fieldPrefix = "field_";
    String valuePrefix = "value_";

    int num = 10;

    for (int i = 0; i < num; i++) {
      String field = fieldPrefix + i;
      String value = valuePrefix + i;
      Map<String, String> m = new HashMap<>();
      m.put(field, value);
      Map<String, ByteIterator> result = StringByteIterator.getByteIteratorMap(m);
      client.insert(tableName, field, result);
    }

    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    Status status = client.scan(tableName, fieldPrefix, 10, null, results);
    assertEquals(Status.OK, status);
    // here we have two field: field_1 and field_2
    assertEquals(10, results.size());

    for (int i = 0; i < num; i++) {
      String field = fieldPrefix + i;
      String value = valuePrefix + i;
      assertEquals(value, results.get(i).get(field).toString());
      status = client.delete(tableName, field);
      assertEquals(Status.OK, status);
    }
  }

}
