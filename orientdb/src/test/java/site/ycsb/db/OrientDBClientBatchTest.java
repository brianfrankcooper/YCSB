/**
 * Copyright (c) 2012 - 2021 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** OrientDB client for YCSB framework. */
public class OrientDBClientBatchTest {
  private static final String CLASS = "usertable";
  private static final int FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String TEST_DB_URL = "memory:test";
  private static final int batchSize = 10;

  private static OrientDBClient OrientDBClient = null;

  @Before
  public void setup() throws DBException {
    OrientDBClient = new OrientDBClient();

    final Properties p = new Properties();
    // TODO: Extract the property names into final variables in OrientDBClient
    p.setProperty("orientdb.url", TEST_DB_URL);
    p.setProperty("batchsize", String.valueOf(10));

    OrientDBClient.setProperties(p);
    OrientDBClient.init();
  }

  @After
  public void teardown() throws DBException {
    if (OrientDBClient != null) {
      OrientDBClient.cleanup();
    }
  }

  /** Inserts a row of deterministic values for the given insertKey using the OrientDBClient. */
  private Map<String, ByteIterator> buildRowBatch(final String insertKey) {
    final Map<String, ByteIterator> insertMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      insertMap.put(
          FIELD_PREFIX + i,
          new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
    }
    return insertMap;
  }

  /**
   * This is a copy of buildDeterministicValue() from core:site.ycsb.workloads.CoreWorkload.java.
   * That method is neither public nor static so we need a copy.
   */
  private String buildDeterministicValue(final String key, final String fieldkey) {
    final int size = FIELD_LENGTH;
    final StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);
    return sb.toString();
  }

  @Test
  public void insertTest10batch10() {
    final int numberRecords = 10;
    for (int i = 0; i < numberRecords; ++i) {
      final String insertKey = "user" + i + "-batch";
      final Map<String, ByteIterator> insertMap = buildRowBatch(insertKey);
      final Status batchStatus = OrientDBClient.insert(CLASS, insertKey, insertMap);
      if (((i + 1) % batchSize) == 0) {
        Assert.assertEquals(Status.OK, batchStatus);
      } else {
        Assert.assertEquals(Status.BATCHED_OK, batchStatus);
      }
    }
  }

  @Test
  public void insertTest20batch10() {
    final int numberRecords = 20;
      for (int i = 0; i < numberRecords; ++i) {
      final String insertKey = "user" + i + "-batch";
      final Map<String, ByteIterator> insertMap = buildRowBatch(insertKey);
      final Status batchStatus = OrientDBClient.insert(CLASS, insertKey, insertMap);
      System.out.println(i);
      if (((i + 1) % batchSize) == 0) {
        Assert.assertEquals(Status.OK, batchStatus);
      } else {
        Assert.assertEquals(Status.BATCHED_OK, batchStatus);
      }
    }
  }
}
