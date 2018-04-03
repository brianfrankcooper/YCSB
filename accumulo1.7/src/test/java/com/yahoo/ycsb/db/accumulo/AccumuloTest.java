/*
 * Copyright (c) 2016 YCSB contributors.
 * All rights reserved.
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

package com.yahoo.ycsb.db.accumulo;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Map.Entry;
import java.util.Properties;

import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.CoreWorkload;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use an Accumulo MiniCluster to test out basic workload operations with
 * the Accumulo binding.
 */
public class AccumuloTest {
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloTest.class);
  private static final int INSERT_COUNT = 2000;
  private static final int TRANSACTION_COUNT = 2000;

  @ClassRule
  public static TemporaryFolder workingDir = new TemporaryFolder();
  @Rule
  public TestName test = new TestName();

  private static MiniAccumuloCluster cluster;
  private static Properties properties;
  private Workload workload;
  private DB client;
  private Properties workloadProps;

  private static boolean isWindows() {
    final String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  @BeforeClass
  public static void setup() throws Exception {
    // Minicluster setup fails on Windows with an UnsatisfiedLinkError.
    // Skip if windows.
    assumeTrue(!isWindows());
    cluster = new MiniAccumuloCluster(workingDir.newFolder("accumulo").getAbsoluteFile(), "protectyaneck");
    LOG.debug("starting minicluster");
    cluster.start();
    LOG.debug("creating connection for admin operations.");
    // set up the table and user
    final Connector admin = cluster.getConnector("root", "protectyaneck");
    admin.tableOperations().create(CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    admin.securityOperations().createLocalUser("ycsb", new PasswordToken("protectyaneck"));
    admin.securityOperations().grantTablePermission("ycsb", CoreWorkload.TABLENAME_PROPERTY_DEFAULT, TablePermission.READ);
    admin.securityOperations().grantTablePermission("ycsb", CoreWorkload.TABLENAME_PROPERTY_DEFAULT, TablePermission.WRITE);

    // set properties the binding will read
    properties = new Properties();
    properties.setProperty("accumulo.zooKeepers", cluster.getZooKeepers());
    properties.setProperty("accumulo.instanceName", cluster.getInstanceName());
    properties.setProperty("accumulo.columnFamily", "family");
    properties.setProperty("accumulo.username", "ycsb");
    properties.setProperty("accumulo.password", "protectyaneck");
    // cut down the batch writer timeout so that writes will push through.
    properties.setProperty("accumulo.batchWriterMaxLatency", "4");
    // set these explicitly to the defaults at the time we're compiled, since they'll be inlined in our class.
    properties.setProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    properties.setProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT);
    properties.setProperty(CoreWorkload.INSERT_ORDER_PROPERTY, "ordered");
  }

  @AfterClass
  public static void clusterCleanup() throws Exception {
    if (cluster != null) {
      LOG.debug("shutting down minicluster");
      cluster.stop();
      cluster = null;
    }
  }

  @Before
  public void client() throws Exception {

    LOG.debug("Loading workload properties for {}", test.getMethodName());
    workloadProps = new Properties();
    workloadProps.load(getClass().getResourceAsStream("/workloads/" + test.getMethodName()));

    for (String prop : properties.stringPropertyNames()) {
      workloadProps.setProperty(prop, properties.getProperty(prop));
    }

    // TODO we need a better test rig for 'run this ycsb workload'
    LOG.debug("initializing measurements and workload");
    Measurements.setProperties(workloadProps);
    workload = new CoreWorkload();
    workload.init(workloadProps);

    LOG.debug("initializing client");
    client = new AccumuloClient();
    client.setProperties(workloadProps);
    client.init();
  }

  @After
  public void cleanup() throws Exception {
    if (client != null) {
      LOG.debug("cleaning up client");
      client.cleanup();
      client = null;
    }
    if (workload != null) {
      LOG.debug("cleaning up workload");
      workload.cleanup();
    }
  }

  @After
  public void truncateTable() throws Exception {
    if (cluster != null) {
      LOG.debug("truncating table {}", CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      final Connector admin = cluster.getConnector("root", "protectyaneck");
      admin.tableOperations().deleteRows(CoreWorkload.TABLENAME_PROPERTY_DEFAULT, null, null);
    }
  }

  @Test
  public void workloada() throws Exception {
    runWorkload();
  }

  @Test
  public void workloadb() throws Exception {
    runWorkload();
  }

  @Test
  public void workloadc() throws Exception {
    runWorkload();
  }

  @Test
  public void workloadd() throws Exception {
    runWorkload();
  }

  @Test
  public void workloade() throws Exception {
    runWorkload();
  }

  /**
   * go through a workload cycle.
   * <ol>
   *   <li>initialize thread-specific state
   *   <li>load the workload dataset
   *   <li>run workload transactions
   * </ol>
   */
  private void runWorkload() throws Exception {
    final Object state = workload.initThread(workloadProps,0,0);
    LOG.debug("load");
    for (int i = 0; i < INSERT_COUNT; i++) {
      assertTrue("insert failed.", workload.doInsert(client, state));
    }
    // Ensure we wait long enough for the batch writer to flush
    // TODO accumulo client should be flushing per insert by default.
    Thread.sleep(2000);
    LOG.debug("verify number of cells");
    final Scanner scanner = cluster.getConnector("root", "protectyaneck").createScanner(CoreWorkload.TABLENAME_PROPERTY_DEFAULT, Authorizations.EMPTY);
    int count = 0;
    for (Entry<Key, Value> entry : scanner) {
      count++;
    }
    assertEquals("Didn't get enough total cells.", (Integer.valueOf(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT) * INSERT_COUNT), count);
    LOG.debug("run");
    for (int i = 0; i < TRANSACTION_COUNT; i++) {
      assertTrue("transaction failed.", workload.doTransaction(client, state));
    }
  }
}
