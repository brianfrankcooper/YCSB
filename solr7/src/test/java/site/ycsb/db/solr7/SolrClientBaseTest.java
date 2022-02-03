/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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

package site.ycsb.db.solr7;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.NamedList;
import org.junit.*;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private final static String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
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
    miniSolrCloudCluster.uploadConfigSet(Paths.get(configDir.toURI()), MOCK_TABLE);
  }

  @AfterClass
  public static void destroy() throws Exception {
    if(miniSolrCloudCluster != null) {
      miniSolrCloudCluster.shutdown();
    }
  }

  @Before
  public void setup() throws Exception {
    CollectionAdminRequest.createCollection(MOCK_TABLE, MOCK_TABLE, 1, 1)
      .withProperty("solr.directoryFactory", "solr.StandardDirectoryFactory")
      .process(miniSolrCloudCluster.getSolrClient());
    miniSolrCloudCluster.waitForActiveCollection(MOCK_TABLE, 1, 1);
    Thread.sleep(1000);

    instance = getDB();
  }

  @After
  public void tearDown() throws Exception {
    if(miniSolrCloudCluster != null) {
      CollectionAdminRequest.deleteCollection(MOCK_TABLE)
        .processAndWait(miniSolrCloudCluster.getSolrClient(), 60);
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
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    Status result = instance.update(MOCK_TABLE, MOCK_KEY1, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    HashMap<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY1, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
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
