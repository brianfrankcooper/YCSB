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

package site.ycsb.db.hbase2;

import static org.junit.Assert.assertArrayEquals;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * Integration tests for the YCSB HBase 2 client using an HBase minicluster.
 */
public class HBaseClient2Test {

  private final static String COLUMN_FAMILY = "cf";

  private static HBaseTestingUtility testingUtil;
  private HBaseClient2 client;
  private Table table = null;
  private String tableName;

  private static boolean isWindows() {
    final String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  /**
   * Creates a mini-cluster for use in these tests.
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
   * Re-create the table for each test. Using default properties.
   */
  public void setUp() throws Exception {
    setUp(new Properties());
  }

  /**
   * Re-create the table for each test. Using custom properties.
   */
  public void setUp(Properties p) throws Exception {
    client = new HBaseClient2();
    client.setConfiguration(new Configuration(testingUtil.getConfiguration()));

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
    testingUtil.deleteTable(TableName.valueOf(tableName));
  }

  @Test
  public void testRead() throws Exception {
    setUp();
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
    setUp();
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(tableName, "Missing row", null, result);
    assertEquals(Status.NOT_FOUND, status);
    assertEquals(0, result.size());
  }

  @Test
  public void testScan() throws Exception {
    setUp();
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
      final HashMap<String, ByteIterator> row = result.get(i);
      assertEquals(1, row.size());
      assertTrue(row.containsKey(colStr));
      final byte[] bytes = row.get(colStr).toArray();
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int rowNum = buf.getInt();
      assertEquals(i + 1, rowNum);
    }
  }

  @Test
  public void testScanWithValueFilteringUsingDefaultProperties() throws Exception {
    testScanWithValueFiltering(null, null, 100, new byte[][] {
        Bytes.fromHex("0000"), Bytes.fromHex("1111"), Bytes.fromHex("2222"), Bytes.fromHex("3333"),
        Bytes.fromHex("4444"), Bytes.fromHex("5555"), Bytes.fromHex("6666"), Bytes.fromHex("7777"),
    });
  }

  @Test
  public void testScanWithValueFilteringOperationLessOrEqual() throws Exception {
    testScanWithValueFiltering("less_or_equal", "3333", 100, new byte[][] {
        Bytes.fromHex("0000"), Bytes.fromHex("1111"), Bytes.fromHex("2222"), Bytes.fromHex("3333"),
    });
  }

  @Test
  public void testScanWithValueFilteringOperationEqual() throws Exception {
    testScanWithValueFiltering("equal", "AAAA", 100, new byte[][]{
        Bytes.fromHex("AAAA")
    });
  }

  @Test
  public void testScanWithValueFilteringOperationNotEqual() throws Exception {
    testScanWithValueFiltering("not_equal", "AAAA", 100 , new byte[][]{
        Bytes.fromHex("0000"), Bytes.fromHex("1111"), Bytes.fromHex("2222"), Bytes.fromHex("3333"),
        Bytes.fromHex("4444"), Bytes.fromHex("5555"), Bytes.fromHex("6666"), Bytes.fromHex("7777"),
        Bytes.fromHex("8888"), Bytes.fromHex("9999"), Bytes.fromHex("BBBB"),
        Bytes.fromHex("CCCC"), Bytes.fromHex("DDDD"), Bytes.fromHex("EEEE"), Bytes.fromHex("FFFF")
    });
  }

  @Test
  public void testScanWithValueFilteringAndRowLimit() throws Exception {
    testScanWithValueFiltering("greater", "8887", 3, new byte[][] {
        Bytes.fromHex("8888"), Bytes.fromHex("9999"), Bytes.fromHex("AAAA")
    });
  }

  @Test
  public void testUpdate() throws Exception{
    setUp();
    final String key = "key";
    final HashMap<String, String> input = new HashMap<String, String>();
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

  private void testScanWithValueFiltering(String operation, String filterValue, int scanRowLimit,
                                          byte[][] expectedValuesReturned) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("hbase.usescanvaluefiltering", String.valueOf(true));
    if(operation != null) {
      properties.setProperty("hbase.scanfilteroperator", operation);
    }
    if(filterValue != null) {
      properties.setProperty("hbase.scanfiltervalue", filterValue);
    }

    // setup the client and fill two columns with data
    setUp(properties);
    setupTableColumnWithHexValues("col_1");
    setupTableColumnWithHexValues("col_2");

    Vector<HashMap<String, ByteIterator>> result = new Vector<>();

    // first scan the whole table (both columns)
    client.scan(tableName, "00000", scanRowLimit, null, result);

    assertEquals(expectedValuesReturned.length, result.size());
    for(int i = 0; i < expectedValuesReturned.length; i++) {
      final HashMap<String, ByteIterator> row = result.get(i);
      assertEquals(2, row.size());
      assertTrue(row.containsKey("col_1") && row.containsKey("col_2"));
      assertArrayEquals(expectedValuesReturned[i], row.get("col_1").toArray());
      assertArrayEquals(expectedValuesReturned[i], row.get("col_2").toArray());
    }

    // now scan only a single column (the filter should work here too)
    result = new Vector<>();
    client.scan(tableName, "00000", scanRowLimit, Collections.singleton("col_1"), result);

    assertEquals(expectedValuesReturned.length, result.size());
    for(int i = 0; i < expectedValuesReturned.length; i++) {
      final HashMap<String, ByteIterator> row = result.get(i);
      assertEquals(1, row.size());
      assertTrue(row.containsKey("col_1"));
      assertArrayEquals(expectedValuesReturned[i], row.get("col_1").toArray());
    }
  }

  private void setupTableColumnWithHexValues(String colStr) throws Exception {
    final byte[] col = Bytes.toBytes(colStr);
    final byte[][] values = {
        Bytes.fromHex("0000"), Bytes.fromHex("1111"), Bytes.fromHex("2222"), Bytes.fromHex("3333"),
        Bytes.fromHex("4444"), Bytes.fromHex("5555"), Bytes.fromHex("6666"), Bytes.fromHex("7777"),
        Bytes.fromHex("8888"), Bytes.fromHex("9999"), Bytes.fromHex("AAAA"), Bytes.fromHex("BBBB"),
        Bytes.fromHex("CCCC"), Bytes.fromHex("DDDD"), Bytes.fromHex("EEEE"), Bytes.fromHex("FFFF")
    };
    final List<Put> puts = new ArrayList<>(16);
    for(int i = 0; i < 16; i++) {
      final byte[] key = Bytes.toBytes(String.format("%05d", i));
      final byte[] value = values[i];
      final Put p = new Put(key);
      p.addColumn(Bytes.toBytes(COLUMN_FAMILY), col, value);
      puts.add(p);
    }
    table.put(puts);
  }

}

