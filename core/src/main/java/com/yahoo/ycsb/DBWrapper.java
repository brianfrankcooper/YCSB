/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB {
  private final DB db;
  private final Measurements measurements;

  public DBWrapper(final DB db) {
    this.db = db;
    measurements = Measurements.getMeasurements();
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    db.cleanup();
    final long en = System.nanoTime();
    measure("CLEANUP", ist, st, en);
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int delete(final String table, final String key) {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    final int res = db.delete(table, key);
    final long en = System.nanoTime();
    measure("DELETE", ist, st, en);
    measurements.reportReturnCode("DELETE", res);
    return res;
  }

  /**
   * Get the set of properties for this DB.
   */
  @Override
  public Properties getProperties() {
    return db.getProperties();
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    db.init();
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int insert(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    final int res = db.insert(table, key, values);
    final long en = System.nanoTime();
    measure("INSERT", ist, st, en);
    measurements.reportReturnCode("INSERT", res);
    return res;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int read(final String table, final String key,
      final Set<String> fields, final HashMap<String, ByteIterator> result) {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    final int res = db.read(table, key, fields, result);
    final long en = System.nanoTime();
    measure("READ", ist, st, en);
    measurements.reportReturnCode("READ", res);
    return res;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int scan(final String table, final String startkey,
      final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    final int res = db.scan(table, startkey, recordcount, fields, result);
    final long en = System.nanoTime();
    measure("SCAN", ist, st, en);
    measurements.reportReturnCode("SCAN", res);
    return res;
  }

  /**
   * Set the properties for this DB.
   */
  @Override
  public void setProperties(final Properties p) {
    db.setProperties(p);
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int update(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    final long ist = measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    final int res = db.update(table, key, values);
    final long en = System.nanoTime();
    measure("UPDATE", ist, st, en);
    measurements.reportReturnCode("UPDATE", res);
    return res;
  }

  private void measure(final String op, final long intendedStartTimeNanos,
      final long startTimeNanos, final long endTimeNanos) {
    measurements.measure(op, (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(op,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }
}
