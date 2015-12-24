/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class GoodBadUglyDB extends DB {
  public static final String SIMULATE_DELAY = "gbudb.delays";
  public static final String SIMULATE_DELAY_DEFAULT = "200,1000,10000,50000,100000";
  private static final ReadWriteLock DB_ACCESS = new ReentrantReadWriteLock();
  private long[] delays;

  public GoodBadUglyDB() {
    delays = new long[]{200, 1000, 10000, 50000, 200000};
  }

  private void delay() {
    final Random random = Utils.random();
    double p = random.nextDouble();
    int mod;
    if (p < 0.9) {
      mod = 0;
    } else if (p < 0.99) {
      mod = 1;
    } else if (p < 0.9999) {
      mod = 2;
    } else {
      mod = 3;
    }
    // this will make mod 3 pauses global
    Lock lock = mod == 3 ? DB_ACCESS.writeLock() : DB_ACCESS.readLock();
    if (mod == 3) {
      System.out.println("OUCH");
    }
    lock.lock();
    try {
      final long baseDelayNs = MICROSECONDS.toNanos(delays[mod]);
      final int delayRangeNs = (int) (MICROSECONDS.toNanos(delays[mod + 1]) - baseDelayNs);
      final long delayNs = baseDelayNs + random.nextInt(delayRangeNs);
      final long deadline = System.nanoTime() + delayNs;
      do {
        LockSupport.parkNanos(deadline - System.nanoTime());
      } while (System.nanoTime() < deadline && !Thread.interrupted());
    } finally {
      lock.unlock();
    }

  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() {
    int i = 0;
    for (String delay : getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT).split(",")) {
      delays[i++] = Long.parseLong(delay);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    delay();
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    delay();

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    delay();

    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    delay();
    return Status.OK;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public Status delete(String table, String key) {
    delay();
    return Status.OK;
  }
}
