/**
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

import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static com.yahoo.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.CoreWorkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * Integration tests for the YCSB HBase client 1.0, using an HBase minicluster.
 */
public class HBaseClient10Test {

  private final static String COLUMN_FAMILY = "cf";

  private static HBaseTestingUtility testingUtil;
  private HBaseClient10 client;
  private Table table = null;
  private String tableName;

  private static boolean isWindows() {
    final String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  /**
   * Creates a mini-cluster for use in these tests.
   *
   * This is a heavy-weight operation, so invoked only once for the test class.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // Minicluster setup fails on Windows with an UnsatisfiedLinkError.
    // Skip if windows.
    assumeTrue(!isWindows());
    testingUtil = HBaseTestingUtility.createLocalHTU();
    testingUtil.startMiniCluster();
  }

  /**
   * Tears down mini-cluster.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    if (testingUtil != null) {
      testingUtil.shutdownMiniCluster();
    }
  }

  /**
   * Sets up the mini-cluster for testing.
   *
   * We re-create the table for each test.
   */
  @Before
  public void setUp() throws Exception {
    client = new HBaseClient10();
    client.setConfiguration(new Configuration(testingUtil.getConfiguration()));

    Properties p = new Properties();
    p.setProperty("columnfamily", COLUMN_FAMILY);

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    table = testingUtil.createTable(TableName.valueOf(tableName), Bytes.toBytes(COLUMN_FAMILY));

    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDown() throws Exception {
    table.close();
    testingUtil.deleteTable(tableName);
  }

  @Test
  public void testRead() throws Exception {
    final String rowKey = "row1";
    final Put p = new Put(Bytes.toBytes(rowKey));
    p.addColumn(Bytes.toBytes(COLUMN_FAMILY),
        Bytes.toBytes("column1"), Bytes.toBytes("value1"));
    p.addColumn(Bytes.toBytes(COLUMN_FAMILY),
        Bytes.toBytes("column2"), Bytes.toBytes("value2"));
    table.put(p);

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(tableName, rowKey, null, result);
    assertEquals(Status.OK, status);
    assertEquals(2, result.size());
    assertEquals("value1", result.get("column1").toString());
    assertEquals("value2", result.get("column2").toString());
  }

  @Test
  public void testReadMissingRow() throws Exception {
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(tableName, "Missing row", null, result);
    assertEquals(Status.NOT_FOUND, status);
    assertEquals(0, result.size());
  }

  @Test
  public void testScan() throws Exception {
    // Fill with data
    final String colStr = "row_number";
    final byte[] col = Bytes.toBytes(colStr);
    final int n = 10;
    final List<Put> puts = new ArrayList<Put>(n);
    for(int i = 0; i < n; i++) {
      final byte[] key = Bytes.toBytes(String.format("%05d", i));
      final byte[] value = java.nio.ByteBuffer.allocate(4).putInt(i).array();
      final Put p = new Put(key);
      p.addColumn(Bytes.toBytes(COLUMN_FAMILY), col, value);
      puts.add(p);
    }
    table.put(puts);

    // Test
    final Vector<HashMap<String, ByteIterator>> result =
        new Vector<HashMap<String, ByteIterator>>();

    // Scan 5 records, skipping the first
    client.scan(tableName, "00001", 5, null, result);

    assertEquals(5, result.size());
    for(int i = 0; i < 5; i++) {
      final Map<String, ByteIterator> row = result.get(i);
      assertEquals(1, row.size());
      assertTrue(row.containsKey(colStr));
      final byte[] bytes = row.get(colStr).toArray();
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int rowNum = buf.getInt();
      assertEquals(i + 1, rowNum);
    }
  }

  @Test
  public void testUpdate() throws Exception{
    final String key = "key";
    final Map<String, String> input = new HashMap<String, String>();
    input.put("column1", "value1");
    input.put("column2", "value2");
    final Status status = client.insert(tableName, key, StringByteIterator.getByteIteratorMap(input));
    assertEquals(Status.OK, status);

    // Verify result
    final Get get = new Get(Bytes.toBytes(key));
    final Result result = this.table.get(get);
    assertFalse(result.isEmpty());
    assertEquals(2, result.size());
    for(final java.util.Map.Entry<String, String> entry : input.entrySet()) {
      assertEquals(entry.getValue(),
          new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY),
            Bytes.toBytes(entry.getKey()))));
    }
  }

  @Test
  @Ignore("Not yet implemented")
  public void testDelete() {
    fail("Not yet implemented");
  }
}

