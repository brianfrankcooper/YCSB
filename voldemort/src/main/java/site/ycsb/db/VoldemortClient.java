/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

/**
 * YCSB binding for
 * <a href="http://www.project-voldemort.com/voldemort/">Voldemort</a>.
 */
public class VoldemortClient extends DB {
  private static final Logger LOGGER = Logger.getLogger(VoldemortClient.class);

  private StoreClient<String, HashMap<String, String>> storeClient;
  private SocketStoreClientFactory socketFactory;
  private String storeName;

  /**
   * Initialize the DB layer. This accepts all properties allowed by the
   * Voldemort client. A store maps to a table. Required : bootstrap_urls
   * Additional property : store_name -> to preload once, should be same as -t
   * {@link ClientConfig}
   */
  public void init() throws DBException {
    ClientConfig clientConfig = new ClientConfig(getProperties());
    socketFactory = new SocketStoreClientFactory(clientConfig);

    // Retrieve store name
    storeName = getProperties().getProperty("store_name", "usertable");

    // Use store name to retrieve client
    storeClient = socketFactory.getStoreClient(storeName);
    if (storeClient == null) {
      throw new DBException("Unable to instantiate store client");
    }
  }

  public void cleanup() throws DBException {
    socketFactory.close();
  }

  @Override
  public Status delete(String table, String key) {
    if (checkStore(table) == Status.ERROR) {
      return Status.ERROR;
    }

    if (storeClient.delete(key)) {
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (checkStore(table) == Status.ERROR) {
      return Status.ERROR;
    }
    storeClient.put(key,
        (HashMap<String, String>) StringByteIterator.getStringMap(values));
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (checkStore(table) == Status.ERROR) {
      return Status.ERROR;
    }

    Versioned<HashMap<String, String>> versionedValue = storeClient.get(key);

    if (versionedValue == null) {
      return Status.NOT_FOUND;
    }

    if (fields != null) {
      for (String field : fields) {
        String val = versionedValue.getValue().get(field);
        if (val != null) {
          result.put(field, new StringByteIterator(val));
        }
      }
    } else {
      StringByteIterator.putAllAsByteIterators(result,
          versionedValue.getValue());
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.warn("Voldemort does not support Scan semantics");
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    if (checkStore(table) == Status.ERROR) {
      return Status.ERROR;
    }

    Versioned<HashMap<String, String>> versionedValue = storeClient.get(key);
    HashMap<String, String> value = new HashMap<String, String>();
    VectorClock version;
    if (versionedValue != null) {
      version = ((VectorClock) versionedValue.getVersion()).incremented(0, 1);
      value = versionedValue.getValue();
      for (Entry<String, ByteIterator> entry : values.entrySet()) {
        value.put(entry.getKey(), entry.getValue().toString());
      }
    } else {
      version = new VectorClock();
      StringByteIterator.putAllAsStrings(value, values);
    }

    storeClient.put(key, Versioned.value(value, version));
    return Status.OK;
  }

  private Status checkStore(String table) {
    if (table.compareTo(storeName) != 0) {
      try {
        storeClient = socketFactory.getStoreClient(table);
        if (storeClient == null) {
          LOGGER.error("Could not instantiate storeclient for " + table);
          return Status.ERROR;
        }
        storeName = table;
      } catch (Exception e) {
        return Status.ERROR;
      }
    }
    return Status.OK;
  }

}
