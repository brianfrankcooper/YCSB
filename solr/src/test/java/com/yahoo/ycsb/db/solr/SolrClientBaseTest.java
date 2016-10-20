/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.solr;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.NamedList;
import org.junit.*;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class SolrClientBaseTest {

  protected static MiniSolrCloudCluster miniSolrCloudCluster;
  private DB instance;
  private final static HashMap<String, ByteIterator> MOCK_DATA;
  protected final static String MOCK_TABLE = "ycsb";
  private final static String MOCK_KEY0 = "0";
  private final static String MOCK_KEY1 = "1";
  private final static int NUM_RECORDS = 10;

  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put("field" + i, new StringByteIterator("value" + i));
    }
  }

  @BeforeClass
  public static void onlyOnce() throws Exception {
    Path miniSolrCloudClusterTempDirectory = Files.createTempDirectory("miniSolrCloudCluster");
    miniSolrCloudClusterTempDirectory.toFile().deleteOnExit();
    miniSolrCloudCluster = new MiniSolrCloudCluster(1, miniSolrCloudClusterTempDirectory, JettyConfig.builder().build());

    // Upload Solr configuration
    URL configDir = SolrClientBaseTest.class.getClassLoader().getResource("solr_config");
    assertNotNull(configDir);
    miniSolrCloudCluster.uploadConfigDir(new File(configDir.toURI()), MOCK_TABLE);
  }

  @AfterClass
  public static void destroy() throws Exception {
    if(miniSolrCloudCluster != null) {
      miniSolrCloudCluster.shutdown();
    }
  }

  @Before
  public void setup() throws Exception {
    NamedList<Object> namedList = miniSolrCloudCluster.createCollection(MOCK_TABLE, 1, 1, MOCK_TABLE, null);
    assertEquals(namedList.indexOf("success", 0), 1);
    Thread.sleep(1000);

    instance = getDB();
  }

  @After
  public void tearDown() throws Exception {
    if(miniSolrCloudCluster != null) {
      NamedList<Object> namedList = miniSolrCloudCluster.deleteCollection(MOCK_TABLE);
      assertEquals(namedList.indexOf("success", 0), 1);
      Thread.sleep(1000);
    }
  }

  @Test
  public void testInsert() throws Exception {
    Status result = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
    assertEquals(Status.OK, result);
  }

  @Test
  public void testDelete() throws Exception {
    Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
    assertEquals(Status.OK, result);
  }

  @Test
  public void testRead() throws Exception {
    Set<String> fields = MOCK_DATA.keySet();
    HashMap<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    Status result = instance.read(MOCK_TABLE, MOCK_KEY1, fields, resultParam);
    assertEquals(Status.OK, result);
  }

  @Test
  public void testUpdate() throws Exception {
    HashMap<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put("field" + i, new StringByteIterator("newvalue" + i));
    }

    Status result = instance.update(MOCK_TABLE, MOCK_KEY1, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    HashMap<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY1, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
    }
  }

  @Test
  public void testScan() throws Exception {
    Set<String> fields = MOCK_DATA.keySet();
    Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
    Status result = instance.scan(MOCK_TABLE, MOCK_KEY1, NUM_RECORDS, fields, resultParam);
    assertEquals(Status.OK, result);
  }

  /**
   * Gets the test DB.
   *
   * @return The test DB.
   */
  protected DB getDB() {
    return getDB(new Properties());
  }

  /**
   * Gets the test DB.
   *
   * @param props
   *    Properties to pass to the client.
   * @return The test DB.
   */
  protected abstract DB getDB(Properties props);
}
