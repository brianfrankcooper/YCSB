/**
 * Copyright (c) 2019 YCSB contributors. All rights reserved.
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

package site.ycsb.db;

import site.ycsb.*;
import site.ycsb.Status;
import site.ycsb.ByteIterator;
import site.ycsb.ByteArrayByteIterator;

import com.penglai.hetu.Hetu;
import com.penglai.hetu.HetuException;
import com.penglai.hetu.HetuItem;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Hetu binding for <a href="http://hetu.penglai.com/">Hetu</a>
 *
 * See {@code rocksdb/README.md} for details.
 */
public class HetuClient extends DB {

  private static String url;
  private static Map<String, Hetu> he = new HashMap<String, Hetu>();
  private HetuItem item;
  private static Integer count;
  private static Object lock = new Object();

  /**
   * Initialize HetuClient.
   */
  public void init() throws DBException {
    url = "he://" + getProperties().getProperty("hetu.deviceAddress") +
          "/" + getProperties().getProperty("hetu.devicePath");
    item = new HetuItem();
    synchronized(lock) {
      if(count == null) {
        count = new Integer(0);
      }
      count++;
    }
  }

  /**
   * Cleanup HetuClient.
   */
  public void cleanup() throws DBException {
    closeall();
  }

  private void closeall() throws HetuException {
    synchronized(this) {
      count--;
      if(count.intValue() == 0) {
        for(Map.Entry<String, Hetu> datastore : he.entrySet()) {
          datastore.getValue().commit();
          datastore.getValue().close();
        }
      } else {
        for(Map.Entry<String, Hetu> datastore : he.entrySet()) {
          datastore.getValue().commit();
        }
      }
    }
  }

  /**
   * Read a single record.
   */
  public Status read(String table, String key, Set<String> fields,
                  Map<String, ByteIterator> result) throws HetuException {
    if(!he.containsKey(table)) {
      addDatastore(table);
    }
    Hetu hetu = he.get(table);
    item.setKeyBytes(key.getBytes());
    item.setValueLength(Hetu.HE_MAX_VAL_LEN);
    hetu.lookup(item);
    fillWithFieldsFrom(result, fields, item.getValueBytes());
    return Status.OK;
  }

  /**
   * Perform a range scan.
   */
  public Status scan(String table, String startKey, int recordcount,
                  Set<String> fields, Vector<HashMap<String, ByteIterator>> result) throws HetuException {
    if(!he.containsKey(table)) {
      addDatastore(table);
    }
    Hetu hetu = he.get(table);
    item.setKeyBytes(startKey.getBytes());
    item.setValueLength(Hetu.HE_MAX_VAL_LEN);
    hetu.lookup(item);
    HashMap<String, ByteIterator> first = new HashMap<String, ByteIterator>();
    fillWithFieldsFrom(first, fields, item.getValueBytes());
    result.add(first);
    for(int i = 1; i < recordcount; i++) {
      HashMap<String, ByteIterator> entry = new HashMap<String, ByteIterator>();
      item.setValueLength(Hetu.HE_MAX_VAL_LEN);
      hetu.next(item);
      fillWithFieldsFrom(entry, fields, item.getValueBytes());
      result.add(entry);
    }
    return Status.OK;
  }

  /**
   * Update a single record.
   */
  public Status update(String table, String key, Map<String, ByteIterator> values) throws HetuException {
    if(!he.containsKey(table)) {
      addDatastore(table);
    }
    Hetu hetu = he.get(table);
    item.setKeyBytes(key.getBytes());
    try {
      item.setValueBytes(getBytesFrom(values));
    } catch (IOException e) {
      e.printStackTrace();
    }
    hetu.update(item);
    return Status.OK;
  }

  /**
   * Insert a single record.
   */
  public Status insert(String table, String key, Map<String, ByteIterator> values) throws HetuException {
    if(!he.containsKey(table)) {
      addDatastore(table);
    }
    Hetu hetu = he.get(table);
    item.setKeyBytes(key.getBytes());
    try {
      item.setValueBytes(getBytesFrom(values));
    } catch (IOException e) {
      e.printStackTrace();
    }
    hetu.insert(item);
    return Status.OK;
  }

  /**
   * Delete a single record.
   */
  public Status delete(String table, String key) throws HetuException {
    if(!he.containsKey(table)) {
      addDatastore(table);
    }
    Hetu hetu = he.get(table);
    item.setKeyBytes(key.getBytes());
    hetu.delete(item);
    return Status.OK;
  }

  /**
   * Helper functions to add new tables.
   */
  private void addDatastore(String table) throws HetuException {
    synchronized(lock) {
      if(!he.containsKey(table)) {
        he.put(table, new Hetu(url, table, Hetu.HE_O_CREATE));
      }
    }
  }

  /**
   * Convert from hetu to ycsb.
   */
  private static void fillWithFieldsFrom(Map<String, ByteIterator> destination, Set<String> fields, byte[] origin) {
    final ByteBuffer tmp = ByteBuffer.allocate(4);
    int index = 0;
    while(index < origin.length) {
      tmp.put(origin, index, 4);
      tmp.flip();
      final int keyLen = tmp.getInt();
      tmp.clear();
      index += 4;
      final String key = new String(origin, index, keyLen);
      index += keyLen;

      tmp.put(origin, index, 4);
      tmp.flip();
      final int valLen = tmp.getInt();
      tmp.clear();
      index += 4;
      if(fields == null || fields.contains(key)) {
        destination.put(key, new ByteArrayByteIterator(origin, index, valLen));
      }
      index += valLen;
    }
  }

  /**
   * Convert from ycsb to hetu.
   */
  private static byte[] getBytesFrom(Map<String, ByteIterator> origin) throws IOException {
    try(final ByteArrayOutputStream res = new ByteArrayOutputStream()) {
      final ByteBuffer tmp = ByteBuffer.allocate(4);
      for(final Map.Entry<String, ByteIterator> entry : origin.entrySet()) {
        final byte[] key = entry.getKey().getBytes(UTF_8);
        final byte[] val = entry.getValue().toArray();

        tmp.putInt(key.length);
        res.write(tmp.array());
        res.write(key);
        tmp.clear();

        tmp.putInt(val.length);
        res.write(tmp.array());
        res.write(val);
        tmp.clear();
      }
      return res.toByteArray();
    }
  }
}
