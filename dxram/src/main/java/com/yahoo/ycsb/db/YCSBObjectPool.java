package com.yahoo.ycsb.db;

/**
 * Pool for YCSBObjects to avoid allocations.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
class YCSBObjectPool {
  private YCSBObject[] pool;

  YCSBObjectPool(final int threads, final int numFields, final int fieldSize) {
    // threads don't start with thread id 0
    pool = new YCSBObject[threads + 100];

    for (int i = 0; i < pool.length; i++) {
      pool[i] = new YCSBObject(numFields, fieldSize);
    }
  }

  YCSBObject get() {
    int idx = (int) Thread.currentThread().getId();

    return pool[idx];
  }
}
