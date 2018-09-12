/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db.rocksdb;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 * <p>
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  private static final String PROPERTY_OPTIONS_FILE = "options.file";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  @GuardedBy("RocksDBClient.class")
  private static Path rocksDbDir;

  @GuardedBy("RocksDBClient.class")
  private static RocksObject dbOptions;

  @GuardedBy("RocksDBClient.class")
  private static RocksDB rocksDb;

  @GuardedBy("RocksDBClient.class")
  private static int references;

  private ColumnFamilyOptions cfOptions;

  private static ConcurrentMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
  private static ConcurrentMap<String, Lock> columnFamilyLocks = new ConcurrentHashMap<>();


  @Override
  public void init() throws DBException {
    synchronized (RocksDBClient.class) {
      if (rocksDb == null) {
        rocksDbDir = Paths.get(getProperties().getProperty(PROPERTY_ROCKSDB_DIR));
        LOGGER.info("RocksDB data dir: " + rocksDbDir);

        cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();

        try {
          rocksDb = initRocksDB();
        } catch (final RocksDBException e) {
          throw new DBException(e);
        }
      }

      references++;
    }
  }

  private RocksDB initDefaultDB(int rocksThreads) throws RocksDBException {
    final Options options = new Options()
        .optimizeLevelStyleCompaction()
        .setCreateIfMissing(true)
        .setIncreaseParallelism(rocksThreads)
        .setMaxBackgroundCompactions(rocksThreads)
        .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
    dbOptions = options;
    return RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
  }

  /**
   * Initializes and opens the RocksDB database.
   * <p>
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDB() throws RocksDBException {
    String optionsFileName = getProperties().getProperty(PROPERTY_OPTIONS_FILE,
        OptionsUtil.getLatestOptionsFileName(
            rocksDbDir.toAbsolutePath().toString(), Env.getDefault()));

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if (optionsFileName.isEmpty()) {
      return initDefaultDB(rocksThreads);
    }

    dbOptions = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();

    // We don't wnat to hide incompatible options
    OptionsUtil.loadOptionsFromFile(
        rocksDbDir.resolve(optionsFileName).toAbsolutePath().toString(),
        Env.getDefault(),
        (DBOptions) dbOptions, cfDescs);

    LOGGER.info("Found column families: " + cfDescs.stream()
        .map(cf -> new String(cf.columnFamilyName(), UTF_8))
        .collect(Collectors.toList()).toString());

    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    final RocksDB db = RocksDB.open(
        (DBOptions) dbOptions,
        rocksDbDir.toAbsolutePath().toString(),
        cfDescs, cfHandles);

    for (int i = 0; i < cfDescs.size(); i++) {
      columnFamilies.put(new String(cfDescs.get(i).columnFamilyName(), UTF_8), cfHandles.get(i));
    }

    return db;
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (RocksDBClient.class) {
      if (references == 1) {
        for (final ColumnFamilyHandle cfHandle : columnFamilies.values()) {
          cfHandle.close();
        }
        columnFamilies.clear();

        rocksDb.close();
        rocksDb = null;

        dbOptions.close();
        dbOptions = null;

        cfOptions.close();

        rocksDbDir = null;
      }

      references--;
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      if (!columnFamilies.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = columnFamilies.get(table);
      final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      if (values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (!columnFamilies.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cfHandle = columnFamilies.get(table);
      try (final RocksIterator iterator = rocksDb.newIterator(cfHandle)) {
        int iterations = 0;
        for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
             iterator.next()) {
          final HashMap<String, ByteIterator> values = new HashMap<>();
          deserializeValues(iterator.value(), fields, values);
          result.add(values);
          iterations++;
        }
      }

      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    try {
      if (!columnFamilies.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = columnFamilies.get(table);
      final Map<String, ByteIterator> result = new HashMap<>();
      final byte[] currentValues = rocksDb.get(cf, key.getBytes(UTF_8));
      if (currentValues == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(result));

      return Status.OK;

    } catch (final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (!columnFamilies.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = columnFamilies.get(table);
      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(values));

      return Status.OK;
    } catch (final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (!columnFamilies.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = columnFamilies.get(table);
      rocksDb.delete(cf, key.getBytes(UTF_8));

      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    columnFamilyLocks.putIfAbsent(name, new ReentrantLock());

    final Lock l = columnFamilyLocks.get(name);
    l.lock();
    try {
      if (!columnFamilies.containsKey(name)) {
        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions));
        columnFamilies.put(name, cfHandle);
      }
    } finally {
      l.unlock();
    }
  }

  ConcurrentMap<String, ColumnFamilyHandle> getColumnFamilies() {
    return columnFamilies;
  }
}