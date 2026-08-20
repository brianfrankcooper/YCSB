/**
 * Copyright (c) 2013 - 2021 YCSB contributors. All rights reserved.
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

 /*
 * PmemKV client binding for YCSB.
 *
 * https://github.com/pmem/pmemkv-java
 */
package site.ycsb.db;

import io.pmem.pmemkv.*;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.ByteBuffer;
import java.nio.file.*;
import java.io.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

class ByteConverter implements Converter<byte[]> {
  @Override
  public ByteBuffer toByteBuffer(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public byte[] fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] data = new byte[byteBuffer.remaining()];
    byteBuffer.get(data);
    return data;
  }
}

class MapToByteBufferConverter implements Converter<Map<String, ByteIterator>> {

  public MapToByteBufferConverter() {
  }

  @Override
  public ByteBuffer toByteBuffer(Map<String, ByteIterator> entries) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> entry : entries.entrySet()) {
        final byte[] keyBytes = entry.getKey().getBytes(UTF_8);
        final byte[] valueBytes = entry.getValue().toArray();
        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);
        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);
        buf.clear();
      }
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Map<String, ByteIterator> fromByteBuffer(ByteBuffer input) {
    Map<String, ByteIterator> result = new HashMap<>();

    while (input.remaining() != 0) {
      final int keyLen = input.getInt(); /* increments position by 4 */
      byte[] values = new byte[keyLen];
      input.get(values, 0, keyLen);
      final String key = new String(values, 0, keyLen);
      final int valueLen = input.getInt();
      values = new byte[valueLen];
      input.get(values, 0, valueLen);
      result.put(key, new ByteArrayByteIterator(values, 0, valueLen));
    }

    return result;
  }
}

/**
 * A class that wraps the PmemKVClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class PmemKVClient extends DB {
  public static final String ENGINE_PROPERTY = "pmemkv.engine";
  public static final String PATH_PROPERTY = "pmemkv.dbpath";
  public static final String SIZE_PROPERTY = "pmemkv.dbsize";
  public static final String JSON_CONFIG_PROPERTY = "pmemkv.jsonconfigfile";

  private static Database<byte[], Map<String, ByteIterator>> db;
  private static int activeThreads = 0;

  @Override
  public void init() throws DBException {
    synchronized(PmemKVClient.class) {
      if (db == null) {
        Properties props = getProperties();
        /* use cmap as default engine */
        String engineName = props.getProperty(ENGINE_PROPERTY, "cmap");

        String path = props.getProperty(PATH_PROPERTY);
        String size = props.getProperty(SIZE_PROPERTY);
        String jsonConfigProp = props.getProperty(JSON_CONFIG_PROPERTY);
        Path jsonConfigFile;
        String jsonConfigContent = null;
        if (jsonConfigProp != null && !jsonConfigProp.isEmpty()) {
          jsonConfigFile = Paths.get(jsonConfigProp);
          try {
            if (jsonConfigFile != null) {
              jsonConfigContent = new String(Files.readAllBytes(jsonConfigFile), UTF_8);
            }
          } catch (IOException e) {
            throw new DBException(JSON_CONFIG_PROPERTY +
              " contain faulty path or points to improper json config params");
          }
        }

        boolean startError = false;
        Database.Builder<byte[], Map<String, ByteIterator>> builder;

        /* try to open db first */
        try {
          builder = new Database.Builder<byte[], Map<String, ByteIterator>>(engineName);
          if (jsonConfigContent != null) {
            builder = builder.fromJson(jsonConfigContent);
          }
          if (path != null) {
            builder = builder.setPath(path);
          }
          if (size != null) {
            /* It's set just in case, it shouldn't be required to open,
            * but it may be required if someone set up e.g. 'create_if_missing' flag in json file. */
            builder = builder.setSize(Long.parseLong(size));
          }
          db = builder
              .setKeyConverter(new ByteConverter())
              .setValueConverter(new MapToByteBufferConverter())
              .build();
        } catch (DatabaseException e) {
          startError = true;
        }
        if (startError) {
          /* or create it, if it doesn't exist */
          try {
            builder = new Database.Builder<byte[], Map<String, ByteIterator>>(engineName);
            if (jsonConfigContent != null) {
              builder = builder.fromJson(jsonConfigContent);
            }
            if (size != null) {
              builder = builder.setSize(Long.parseLong(size));
            }
            if (path != null) {
              builder = builder.setPath(path);
            }
            db = builder
                .setKeyConverter(new ByteConverter())
                .setValueConverter(new MapToByteBufferConverter())
                .setForceCreate(true)
                .build();
          } catch (DatabaseException e) {
            throw new DBException("Error while open with " + engineName +
                                  ".\nFull error: " + e.getMessage());
          }
        }
      }
      activeThreads++;
    }
  }

  /**
   * Shutdown the client.
   */
  @Override
  public void cleanup() {
    synchronized(PmemKVClient.class) {
      activeThreads--;
      if (activeThreads == 0 && db != null) {
        db.stop();
        db = null;
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      db.get(key.getBytes(UTF_8), result::putAll);
    } catch (NotFoundException e) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    /* TODO(kfilipek): Implement if possible/necessary */
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    final Map<String, ByteIterator> result = new HashMap<>();
    Map<String, ByteIterator> currentValues = db.getCopy(key.getBytes(UTF_8));
    if (currentValues == null) {
      return Status.NOT_FOUND;
    }

    result.putAll(values);

    try {
      db.put(key.getBytes(UTF_8), result);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      db.put(key.getBytes(UTF_8), values);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (!db.remove(key.getBytes(UTF_8))) {
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }
}
