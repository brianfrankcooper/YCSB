/*
 * Copyright (c) 2014, Yahoo!, Inc. All rights reserved.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * MongoDbClientTest provides runs the basic DB test cases.
 * <p>
 * The tests will be skipped if MongoDB is not running on port 27017 on the
 * local machine. See the README.md for how to get MongoDB running.
 * </p>
 */
@SuppressWarnings("boxing")
public abstract class AbstractDBTestCases {

  /** The default port for MongoDB. */
  private static final int MONGODB_DEFAULT_PORT = 27017;

  /**
   * Verifies the mongod process (or some process) is running on port 27017, if
   * not the tests are skipped.
   */
  @BeforeClass
  public static void setUpBeforeClass() {
    // Test if we can connect.
    Socket socket = null;
    try {
      // Connect
      socket = new Socket(InetAddress.getLocalHost(), MONGODB_DEFAULT_PORT);
      assertThat("Socket is not bound.", socket.getLocalPort(), not(-1));
    } catch (IOException connectFailed) {
      assumeNoException("MongoDB is not running. Skipping tests.",
          connectFailed);
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ignore) {
          // Ignore.
        }
      }
      socket = null;
    }
  }

  /**
   * Test method for {@link DB#insert}, {@link DB#read}, and {@link DB#delete} .
   */
  @Test
  public void testInsertReadDelete() {
    final DB client = getDB();

    final String table = getClass().getSimpleName();
    final String id = "delete";

    HashMap<String, ByteIterator> inserted =
        new HashMap<String, ByteIterator>();
    inserted.put("a", new ByteArrayByteIterator(new byte[] { 1, 2, 3, 4 }));
    Status result = client.insert(table, id, inserted);
    assertThat("Insert did not return success (0).", result, is(Status.OK));

    HashMap<String, ByteIterator> read = new HashMap<String, ByteIterator>();
    Set<String> keys = Collections.singleton("a");
    result = client.read(table, id, keys, read);
    assertThat("Read did not return success (0).", result, is(Status.OK));
    for (String key : keys) {
      ByteIterator iter = read.get(key);

      assertThat("Did not read the inserted field: " + key, iter,
          notNullValue());
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 1)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 2)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 3)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 4)));
      assertFalse(iter.hasNext());
    }

    result = client.delete(table, id);
    assertThat("Delete did not return success (0).", result, is(Status.OK));

    read.clear();
    result = client.read(table, id, null, read);
    assertThat("Read, after delete, did not return not found (1).", result,
        is(Status.NOT_FOUND));
    assertThat("Found the deleted fields.", read.size(), is(0));

    result = client.delete(table, id);
    assertThat("Delete did not return not found (1).", result, is(Status.NOT_FOUND));
  }

  /**
   * Test method for {@link DB#insert}, {@link DB#read}, and {@link DB#update} .
   */
  @Test
  public void testInsertReadUpdate() {
    DB client = getDB();

    final String table = getClass().getSimpleName();
    final String id = "update";

    HashMap<String, ByteIterator> inserted =
        new HashMap<String, ByteIterator>();
    inserted.put("a", new ByteArrayByteIterator(new byte[] { 1, 2, 3, 4 }));
    Status result = client.insert(table, id, inserted);
    assertThat("Insert did not return success (0).", result, is(Status.OK));

    HashMap<String, ByteIterator> read = new HashMap<String, ByteIterator>();
    Set<String> keys = Collections.singleton("a");
    result = client.read(table, id, keys, read);
    assertThat("Read did not return success (0).", result, is(Status.OK));
    for (String key : keys) {
      ByteIterator iter = read.get(key);

      assertThat("Did not read the inserted field: " + key, iter,
          notNullValue());
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 1)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 2)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 3)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 4)));
      assertFalse(iter.hasNext());
    }

    HashMap<String, ByteIterator> updated = new HashMap<String, ByteIterator>();
    updated.put("a", new ByteArrayByteIterator(new byte[] { 5, 6, 7, 8 }));
    result = client.update(table, id, updated);
    assertThat("Update did not return success (0).", result, is(Status.OK));

    read.clear();
    result = client.read(table, id, null, read);
    assertThat("Read, after update, did not return success (0).", result, is(Status.OK));
    for (String key : keys) {
      ByteIterator iter = read.get(key);

      assertThat("Did not read the inserted field: " + key, iter,
          notNullValue());
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 5)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 6)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 7)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 8)));
      assertFalse(iter.hasNext());
    }
  }

  /**
   * Test method for {@link DB#insert}, {@link DB#read}, and {@link DB#update} .
   */
  @Test
  public void testInsertReadUpdateWithUpsert() {
    Properties props = new Properties();
    props.setProperty("mongodb.upsert", "true");
    DB client = getDB(props);

    final String table = getClass().getSimpleName();
    final String id = "updateWithUpsert";

    HashMap<String, ByteIterator> inserted =
        new HashMap<String, ByteIterator>();
    inserted.put("a", new ByteArrayByteIterator(new byte[] { 1, 2, 3, 4 }));
    Status result = client.insert(table, id, inserted);
    assertThat("Insert did not return success (0).", result, is(Status.OK));

    HashMap<String, ByteIterator> read = new HashMap<String, ByteIterator>();
    Set<String> keys = Collections.singleton("a");
    result = client.read(table, id, keys, read);
    assertThat("Read did not return success (0).", result, is(Status.OK));
    for (String key : keys) {
      ByteIterator iter = read.get(key);

      assertThat("Did not read the inserted field: " + key, iter,
          notNullValue());
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 1)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 2)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 3)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 4)));
      assertFalse(iter.hasNext());
    }

    HashMap<String, ByteIterator> updated = new HashMap<String, ByteIterator>();
    updated.put("a", new ByteArrayByteIterator(new byte[] { 5, 6, 7, 8 }));
    result = client.update(table, id, updated);
    assertThat("Update did not return success (0).", result, is(Status.OK));

    read.clear();
    result = client.read(table, id, null, read);
    assertThat("Read, after update, did not return success (0).", result, is(Status.OK));
    for (String key : keys) {
      ByteIterator iter = read.get(key);

      assertThat("Did not read the inserted field: " + key, iter,
          notNullValue());
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 5)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 6)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 7)));
      assertTrue(iter.hasNext());
      assertThat(iter.nextByte(), is(Byte.valueOf((byte) 8)));
      assertFalse(iter.hasNext());
    }
  }

  /**
   * Test method for {@link DB#scan}.
   */
  @Test
  public void testScan() {
    final DB client = getDB();

    final String table = getClass().getSimpleName();

    // Insert a bunch of documents.
    for (int i = 0; i < 100; ++i) {
      HashMap<String, ByteIterator> inserted =
          new HashMap<String, ByteIterator>();
      inserted.put("a", new ByteArrayByteIterator(new byte[] {
          (byte) (i & 0xFF), (byte) (i >> 8 & 0xFF), (byte) (i >> 16 & 0xFF),
          (byte) (i >> 24 & 0xFF) }));
      Status result = client.insert(table, padded(i), inserted);
      assertThat("Insert did not return success (0).", result, is(Status.OK));
    }

    Set<String> keys = Collections.singleton("a");
    Vector<HashMap<String, ByteIterator>> results =
        new Vector<HashMap<String, ByteIterator>>();
    Status result = client.scan(table, "00050", 5, null, results);
    assertThat("Read did not return success (0).", result, is(Status.OK));
    assertThat(results.size(), is(5));
    for (int i = 0; i < 5; ++i) {
      HashMap<String, ByteIterator> read = results.get(i);
      for (String key : keys) {
        ByteIterator iter = read.get(key);

        assertThat("Did not read the inserted field: " + key, iter,
            notNullValue());
        assertTrue(iter.hasNext());
        assertThat(iter.nextByte(), is(Byte.valueOf((byte) ((i + 50) & 0xFF))));
        assertTrue(iter.hasNext());
        assertThat(iter.nextByte(),
            is(Byte.valueOf((byte) ((i + 50) >> 8 & 0xFF))));
        assertTrue(iter.hasNext());
        assertThat(iter.nextByte(),
            is(Byte.valueOf((byte) ((i + 50) >> 16 & 0xFF))));
        assertTrue(iter.hasNext());
        assertThat(iter.nextByte(),
            is(Byte.valueOf((byte) ((i + 50) >> 24 & 0xFF))));
        assertFalse(iter.hasNext());
      }
    }
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

  /**
   * Creates a zero padded integer.
   * 
   * @param i
   *          The integer to padd.
   * @return The padded integer.
   */
  private String padded(int i) {
    String result = String.valueOf(i);
    while (result.length() < 5) {
      result = "0" + result;
    }
    return result;
  }

}