/**
 * Copyright (c) 2015 YCSB contributors All rights reserved.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.Sets;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.CoreWorkload;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Integration tests for the Cassandra client
 */
public class CassandraCQLClientTest {
  // Change the default Cassandra timeout from 10s to 120s for slow CI machines
  private final static long timeout = 120000L;

  private final static String TABLE = "usertable";
  private final static String HOST = "localhost";
  private final static int PORT = 9142;
  private final static String DEFAULT_ROW_KEY = "user1";

  private CassandraCQLClient client;
  private Session session;

  @ClassRule
  public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(
    new ClassPathCQLDataSet("ycsb.cql", "ycsb"), null, timeout);

  @Before
  public void setUp() throws Exception {
    session = cassandraUnit.getSession();

    Properties p = new Properties();
    p.setProperty("hosts", HOST);
    p.setProperty("port", Integer.toString(PORT));
    p.setProperty("table", TABLE);

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);
    client = new CassandraCQLClient();
    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDownClient() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    client = null;
  }

  @After
  public void clearTable() throws Exception {
    // Clear the table so that each test starts fresh.
    final Statement truncate = QueryBuilder.truncate(TABLE);
    if (cassandraUnit != null) {
      cassandraUnit.getSession().execute(truncate);
    }
  }

  @Test
  public void testReadMissingRow() throws Exception {
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(TABLE, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  private void insertRow() {
    final String rowKey = DEFAULT_ROW_KEY;
    Insert insertStmt = QueryBuilder.insertInto(TABLE);
    insertStmt.value(CassandraCQLClient.YCSB_KEY, rowKey);

    insertStmt.value("field0", "value1");
    insertStmt.value("field1", "value2");
    session.execute(insertStmt);
  }

  @Test
  public void testRead() throws Exception {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, null, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<String, String>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry(CassandraCQLClient.YCSB_KEY, DEFAULT_ROW_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadSingleColumn() throws Exception {
    insertRow();
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Set<String> fields = Sets.newHashSet("field1");
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, fields, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(1));
    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testUpdate() throws Exception {
    final String key = "key";
    final Map<String, String> input = new HashMap<String, String>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    final Select selectStmt =
        QueryBuilder.select("field0", "field1")
            .from(TABLE)
            .where(QueryBuilder.eq(CassandraCQLClient.YCSB_KEY, key))
            .limit(1);

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, notNullValue());
    assertThat(rs.isExhausted(), is(true));
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
  }
}
