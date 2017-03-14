package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.rocksdb.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  private static final String ROCKSDB_DIR = "rocksdb.dir";

  private static Path rocksDbDir = null;
  private static RocksDB rocksDB = null;
  private static final AtomicInteger REFERENCES = new AtomicInteger();
  private static final ConcurrentMap<String, ColumnFamilyHandle> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  @Override
  public void init() throws DBException {
    super.init();
    if(rocksDB == null) {
      synchronized (RocksDBClient.class) {
        if(rocksDB == null) {
          try {
            RocksDB.loadLibrary();

            String rd = getProperties().getProperty(ROCKSDB_DIR);
            if (rd == null) {
              throw new IllegalArgumentException(String.format("property %s is not set", ROCKSDB_DIR));
            }
            this.rocksDbDir = Paths.get(rd);
            if(!Files.exists(rocksDbDir)) {
              Files.createDirectories(rocksDbDir);
            }

            final List<String> cfNames = loadColumnFamilyNames();
            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            for(final String cfName : cfNames) {
              final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
                  cfName.getBytes(StandardCharsets.UTF_8),
                  new ColumnFamilyOptions().optimizeLevelStyleCompaction()
              );
              cfDescriptors.add(cfDescriptor);
            }

            final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

            if(cfDescriptors.isEmpty()) {
              final Options options = new Options()
                  .optimizeLevelStyleCompaction()
                  .setCreateIfMissing(true)
                  .setCreateMissingColumnFamilies(true)
                  .setIncreaseParallelism(rocksThreads)
                  .setMaxBackgroundCompactions(rocksThreads)
                  .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
              this.rocksDB = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
            } else {
              final DBOptions options = new DBOptions()
                  .setCreateIfMissing(true)
                  .setCreateMissingColumnFamilies(true)
                  .setIncreaseParallelism(rocksThreads)
                  .setMaxBackgroundCompactions(rocksThreads)
                  .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
              final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
              this.rocksDB = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
              for(int i = 0; i < cfNames.size(); i++) {
                COLUMN_FAMILIES.put(cfNames.get(i), cfHandles.get(i));
              }
            }
          } catch(final IOException | RocksDBException e) {
            throw new DBException(e);
          }
        }
      }
    }
    REFERENCES.incrementAndGet();
  }

  @Override
  public void cleanup() throws DBException {
    try {
      super.cleanup();
      if (REFERENCES.get() == 1) {
        for (final ColumnFamilyHandle cf : COLUMN_FAMILIES.values()) {
          cf.dispose();
        }
        rocksDB.close();
        saveColumnFamilyNames();
      }
    } catch (final IOException e) {
      throw new DBException(e);
    } finally {
      REFERENCES.decrementAndGet();
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table);
      final byte[] values = rocksDB.get(cf, key.getBytes(StandardCharsets.UTF_8));
      if(values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch(final RocksDBException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table);
      final RocksIterator iterator = rocksDB.newIterator(cf);

      int iterations = 0;
      for(iterator.seek(startkey.getBytes(StandardCharsets.UTF_8)); iterator.isValid() && iterations < recordcount;
          iterator.next()) {
        final HashMap<String, ByteIterator> values = new HashMap<>();
        deserializeValues(iterator.value(), fields, values);
        result.add(values);
        iterations++;
      }

      iterator.dispose();

      return Status.OK;
    } catch(final RocksDBException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final HashMap<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table);
      final HashMap<String, ByteIterator> result = new HashMap<>();
      final byte[] currentValues = rocksDB.get(cf, key.getBytes(StandardCharsets.UTF_8));
      if(currentValues == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      rocksDB.put(cf, key.getBytes(StandardCharsets.UTF_8), serializeValues(result));

      return Status.OK;

    } catch(final RocksDBException | IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final HashMap<String, ByteIterator> values) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table);
      rocksDB.put(cf, key.getBytes(StandardCharsets.UTF_8), serializeValues(values));

      return Status.OK;
    } catch(final RocksDBException | IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table);
      rocksDB.remove(cf, key.getBytes(StandardCharsets.UTF_8));

      return Status.OK;
    } catch(final RocksDBException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void saveColumnFamilyNames() throws IOException {
    try(final PrintWriter writer = new PrintWriter(new FileWriter(rocksDbDir.resolve("cfNames").toFile()))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final File f = rocksDbDir.resolve("cfNames").toFile();
    if(f.exists()) {
      try (final LineNumberReader reader = new LineNumberReader(new FileReader(f))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private HashMap<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
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

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(StandardCharsets.UTF_8);
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
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyHandle cfHandle = rocksDB.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8),
            new ColumnFamilyOptions().optimizeLevelStyleCompaction())
        );
        COLUMN_FAMILIES.put(name, cfHandle);
      }
    } finally {
      l.unlock();
    }
  }
}
