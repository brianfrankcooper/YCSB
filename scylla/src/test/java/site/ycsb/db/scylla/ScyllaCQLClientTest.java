/*
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package site.ycsb.db.scylla;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Sets;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

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
 * Integration tests for the Scylla client
 */
public class ScyllaCQLClientTest {
  // Change the default Scylla timeout from 10s to 120s for slow CI machines
  private final static long timeout = 120000L;

  private final static String TABLE = "usertable";
  private final static String HOST = "localhost";
  private final static int PORT = 9142;
  private final static String DEFAULT_ROW_KEY = "user1";

  private ScyllaCQLClient client;
  private Session session;

  @ClassRule
  public static CassandraCQLUnit unit = new CassandraCQLUnit(
    new ClassPathCQLDataSet("ycsb.cql", "ycsb"), null, timeout);

  @Before
  public void setUp() throws Exception {
    session = unit.getSession();

    Properties p = new Properties();
    p.setProperty("scylla.hosts", HOST);
    p.setProperty("scylla.port", Integer.toString(PORT));
    p.setProperty("scylla.table", TABLE);

    Measurements.setProperties(p);

    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    client = new ScyllaCQLClient();
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
  public void clearTable() {
    // Clear the table so that each test starts fresh.
    final Statement truncate = QueryBuilder.truncate(TABLE);
    if (unit != null) {
      unit.getSession().execute(truncate);
    }
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = client.read(TABLE, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  private void insertRow() {
    Insert insertStmt = QueryBuilder.insertInto(TABLE);
    insertStmt.value(ScyllaCQLClient.YCSB_KEY, DEFAULT_ROW_KEY);

    insertStmt.value("field0", "value1");
    insertStmt.value("field1", "value2");
    session.execute(insertStmt);
  }

  @Test
  public void testRead() {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, null, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry(ScyllaCQLClient.YCSB_KEY, DEFAULT_ROW_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadSingleColumn() {
    insertRow();
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Set<String> fields = Sets.newHashSet("field1");
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, fields, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(1));
    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testInsert() {
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    final Select selectStmt =
        QueryBuilder.select("field0", "field1")
            .from(TABLE)
            .where(QueryBuilder.eq(ScyllaCQLClient.YCSB_KEY, key))
            .limit(1);

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, notNullValue());
    assertThat(rs.isExhausted(), is(true));
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
  }

  @Test
  public void testUpdate() {
    insertRow();
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "new-value1");
    input.put("field1", "new-value2");

    final Status status = client.update(TABLE,
                                        DEFAULT_ROW_KEY,
                                        StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    final Select selectStmt =
        QueryBuilder.select("field0", "field1")
            .from(TABLE)
            .where(QueryBuilder.eq(ScyllaCQLClient.YCSB_KEY, DEFAULT_ROW_KEY))
            .limit(1);

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, notNullValue());
    assertThat(rs.isExhausted(), is(true));
    assertThat(row.getString("field0"), is("new-value1"));
    assertThat(row.getString("field1"), is("new-value2"));
  }

  @Test
  public void testDelete() {
    insertRow();

    final Status status = client.delete(TABLE, DEFAULT_ROW_KEY);
    assertThat(status, is(Status.OK));

    // Verify result
    final Select selectStmt =
        QueryBuilder.select("field0", "field1")
            .from(TABLE)
            .where(QueryBuilder.eq(ScyllaCQLClient.YCSB_KEY, DEFAULT_ROW_KEY))
            .limit(1);

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, nullValue());
  }

  @Test
  public void testPreparedStatements() {
    final int LOOP_COUNT = 3;
    for (int i = 0; i < LOOP_COUNT; i++) {
      testInsert();
      testUpdate();
      testRead();
      testReadSingleColumn();
      testReadMissingRow();
      testDelete();
    }
  }
}
