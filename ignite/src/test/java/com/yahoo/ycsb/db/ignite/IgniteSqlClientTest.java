/**
 * Copyright (c) 2018 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the Ignite client
 */
public class IgniteSqlClientTest extends IgniteClientTestBase {
  private static final String TABLE_NAME = "usertable";
  private final static String HOST = "127.0.0.1";
  private final static String PORTS = "47500..47509";
  private final static String SERVER_NODE_NAME = "YCSB Server Node";
  private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

  /**
   *
   */
  @BeforeClass
  public static void beforeTest() throws IgniteCheckedException {
    IgniteConfiguration igcfg = new IgniteConfiguration();
    igcfg.setIgniteInstanceName(SERVER_NODE_NAME);
    igcfg.setClientMode(false);

    TcpDiscoverySpi disco = new TcpDiscoverySpi();
    Collection<String> adders = new LinkedHashSet<>();
    adders.add(HOST + ":" + PORTS);

    QueryEntity qe = new QueryEntity("java.lang.String", "UserTableType")
              .addQueryField("ycsb_key", "java.lang.String", null)
              .addQueryField("field0", "java.lang.String", null)
              .addQueryField("field1", "java.lang.String", null)
              .addQueryField("field2", "java.lang.String", null)
              .addQueryField("field3", "java.lang.String", null)
              .addQueryField("field4", "java.lang.String", null)
              .addQueryField("field5", "java.lang.String", null)
              .addQueryField("field6", "java.lang.String", null)
              .addQueryField("field7", "java.lang.String", null)
              .addQueryField("field8", "java.lang.String", null)
              .addQueryField("field9", "java.lang.String", null)
              .setKeyFieldName("ycsb_key");

    qe.setTableName("usertable");

    CacheConfiguration ccfg = new CacheConfiguration().setQueryEntities(Collections.singleton(qe))
              .setName(DEFAULT_CACHE_NAME);

    igcfg.setCacheConfiguration(ccfg);

    ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(adders);
    disco.setIpFinder(ipFinder);

    igcfg.setDiscoverySpi(disco);
    igcfg.setNetworkTimeout(2000);

    Log4J2Logger logger = new Log4J2Logger(IgniteSqlClientTest.class.getClassLoader().getResource("log4j2.xml"));
    igcfg.setGridLogger(logger);

    cluster = Ignition.start(igcfg);
    cluster.active();
  }

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("hosts", HOST);
    p.setProperty("ports", PORTS);
    Measurements.setProperties(p);

    client = new IgniteSqlClient();
    client.setProperties(p);
    client.init();
  }

  @Test
  public void testInsert() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2");
    final Status status = client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));
  }

  @Test
  public void testDelete() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key1 = "key1";
    final Map<String, String> input1 = new HashMap<>();
    input1.put("field0", "value1");
    input1.put("field1", "value2");
    final Status status1 = client.insert(TABLE_NAME, key1, StringByteIterator.getByteIteratorMap(input1));
    assertThat(status1, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final String key2 = "key2";
    final Map<String, String> input2 = new HashMap<>();
    input2.put("field0", "value1");
    input2.put("field1", "value2");
    final Status status2 = client.insert(TABLE_NAME, key2, StringByteIterator.getByteIteratorMap(input2));
    assertThat(status2, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(2));

    final Status status3 = client.delete(TABLE_NAME, key2);
    assertThat(status3, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

  }

  @Test
  public void testRead() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    final Status sPut = client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(sPut, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final Set<String> fld = new TreeSet<>();
    fld.add("field0");
    fld.add("field1");
    fld.add("field3");

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status sGet = client.read(TABLE_NAME, key, fld, result);
    assertThat(sGet, is(Status.OK));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2A"));
  }

  @Test
  public void testUpdate() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));

    input.put("field1", "value2B");
    input.put("field4", "value4A");

    final Status sUpd = client.update(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(sUpd, is(Status.OK));

    final Set<String> fld = new TreeSet<>();
    fld.add("field0");
    fld.add("field1");
    fld.add("field3");
    fld.add("field4");

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status sGet = client.read(TABLE_NAME, key, fld, result);
    assertThat(sGet, is(Status.OK));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2B"));
    assertThat(strResult, hasEntry("field4", "value4A"));
  }

  @Test
  public void testConcurrentUpdate() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));

    input.put("field1", "value2B");
    input.put("field4", "value4A");

    ExecutorService exec = Executors.newCachedThreadPool();

    final AtomicLong l = new AtomicLong(0);
    final Boolean[] updError = {false};

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < 100; ++i) {
          input.put("field1", "value2B_" + l.incrementAndGet());
          final Status sUpd = client.update(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));

          if (!sUpd.isOk()) {
            updError[0] = true;
            break;
          }
        }
      }
    };

    for (int i = 0; i < 32; ++i) {
      exec.execute(task);
    }

    exec.awaitTermination(60, TimeUnit.SECONDS);
    exec.shutdownNow();

    assertThat(updError[0], is(false));
  }

  @Test
  public void testReadAllFields() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    final Status sPut = client.insert(DEFAULT_CACHE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(sPut, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final Set<String> fld = new TreeSet<>();

    final HashMap<String, ByteIterator> result1 = new HashMap<>();
    final Status sGet = client.read(TABLE_NAME, key, fld, result1);
    assertThat(sGet, is(Status.OK));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result1.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2A"));
  }

  @Test
  public void testReadNotPresent() throws Exception {
    cluster.cache(DEFAULT_CACHE_NAME).clear();
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2A");
    input.put("field3", null);
    final Status sPut = client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(sPut, is(Status.OK));
    assertThat(cluster.cache(DEFAULT_CACHE_NAME).size(), is(1));

    final Set<String> fld = new TreeSet<>();

    final String newKey = "newKey";
    final HashMap<String, ByteIterator> result1 = new HashMap<>();
    final Status sGet = client.read(TABLE_NAME, newKey, fld, result1);
    assertThat(sGet, is(Status.NOT_FOUND));

  }
}
