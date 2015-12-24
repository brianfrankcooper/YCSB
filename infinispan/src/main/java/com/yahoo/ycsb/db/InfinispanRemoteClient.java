/**
 * Copyright (c) 2015-2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * This is a client implementation for Infinispan 5.x in client-server mode.
 */
public class InfinispanRemoteClient extends DB {

  private static final Log LOGGER = LogFactory.getLog(InfinispanRemoteClient.class);

  private RemoteCacheManager remoteIspnManager;
  private String cacheName = null;

  @Override
  public void init() throws DBException {
    remoteIspnManager = RemoteCacheManagerHolder.getInstance(getProperties());
    cacheName = getProperties().getProperty("cache");
  }

  @Override
  public void cleanup() {
    remoteIspnManager.stop();
    remoteIspnManager = null;
  }

  @Override
  public Status insert(String table, String recordKey, Map<String, ByteIterator> values) {
    String compositKey = createKey(table, recordKey);
    Map<String, String> stringValues = new HashMap<>();
    StringByteIterator.putAllAsStrings(stringValues, values);
    try {
      cache().put(compositKey, stringValues);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(String table, String recordKey, Set<String> fields, Map<String, ByteIterator> result) {
    String compositKey = createKey(table, recordKey);
    try {
      Map<String, String> values = cache().get(compositKey);

      if (values == null || values.isEmpty()) {
        return Status.NOT_FOUND;
      }

      if (fields == null) { //get all field/value pairs
        StringByteIterator.putAllAsByteIterators(result, values);
      } else {
        for (String field : fields) {
          String value = values.get(field);
          if (value != null) {
            result.put(field, new StringByteIterator(value));
          }
        }
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.warn("Infinispan does not support scan semantics");
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String recordKey, Map<String, ByteIterator> values) {
    String compositKey = createKey(table, recordKey);
    try {
      Map<String, String> stringValues = new HashMap<>();
      StringByteIterator.putAllAsStrings(stringValues, values);
      cache().put(compositKey, stringValues);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String recordKey) {
    String compositKey = createKey(table, recordKey);
    try {
      cache().remove(compositKey);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  private RemoteCache<String, Map<String, String>> cache() {
    if (this.cacheName != null) {
      return remoteIspnManager.getCache(cacheName);
    } else {
      return remoteIspnManager.getCache();
    }
  }

  private String createKey(String table, String recordKey) {
    return table + "-" + recordKey;
  }
}
