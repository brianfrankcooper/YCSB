/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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
 * <p>
 * ZooKeeper client binding for YCSB.
 * <p>
 */

package site.ycsb.db.zookeeper;

import java.io.IOException;
import java.nio.charset.Charset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YCSB binding for <a href="https://zookeeper.apache.org/">ZooKeeper</a>.
 *
 * See {@code zookeeper/README.md} for details.
 */
public class ZKClient extends DB {

  private ZooKeeper zk;
  private Watcher watcher;

  private static final String CONNECT_STRING = "zookeeper.connectString";
  private static final String DEFAULT_CONNECT_STRING = "127.0.0.1:2181";
  private static final String SESSION_TIMEOUT_PROPERTY = "zookeeper.sessionTimeout";
  private static final long DEFAULT_SESSION_TIMEOUT = TimeUnit.SECONDS.toMillis(30L);
  private static final String WATCH_FLAG = "zookeeper.watchFlag";

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);

  public void init() throws DBException {
    Properties props = getProperties();

    String connectString = props.getProperty(CONNECT_STRING);
    if (connectString == null || connectString.length() == 0) {
      connectString = DEFAULT_CONNECT_STRING;
    }

    if(Boolean.parseBoolean(props.getProperty(WATCH_FLAG))) {
      watcher = new SimpleWatcher();
    } else {
      watcher = null;
    }

    long sessionTimeout;
    String sessionTimeoutString = props.getProperty(SESSION_TIMEOUT_PROPERTY);
    if (sessionTimeoutString != null) {
      sessionTimeout = Integer.parseInt(sessionTimeoutString);
    } else {
      sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    }

    try {
      zk = new ZooKeeper(connectString, (int) sessionTimeout, new SimpleWatcher());
    } catch (IOException e) {
      throw new DBException("Creating connection failed.");
    }
  }

  public void cleanup() throws DBException {
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String path = getPath(key);
    try {
      byte[] data = zk.getData(path, watcher, null);
      if (data == null || data.length == 0) {
        return Status.NOT_FOUND;
      }

      deserializeValues(data, fields, result);
      return Status.OK;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when reading a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    String path = getPath(key);
    String data = getJsonStrFromByteMap(values);
    try {
      zk.create(path, data.getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      return Status.OK;
    } catch (KeeperException.NodeExistsException e1) {
      return Status.OK;
    } catch (KeeperException | InterruptedException e2) {
      LOG.error("Error when inserting a path:{},tableName:{}", path, table, e2);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {

    String path = getPath(key);
    try {
      zk.delete(path, -1);
      return Status.OK;
    } catch (InterruptedException | KeeperException e) {
      LOG.error("Error when deleting a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    String path = getPath(key);
    try {
      // we have to do a read operation here before setData to meet the YCSB's update semantics:
      // update a single record in the database, adding or replacing the specified fields.
      byte[] data = zk.getData(path, watcher, null);
      if (data == null || data.length == 0) {
        return Status.NOT_FOUND;
      }
      final Map<String, ByteIterator> result = new HashMap<>();
      deserializeValues(data, null, result);
      result.putAll(values);
      // update
      zk.setData(path, getJsonStrFromByteMap(result).getBytes(UTF_8), -1);
      return Status.OK;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when updating a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  private String getPath(String key) {
    return key.startsWith("/") ? key : "/" + key;
  }

  /**
   * converting the key:values map to JSON Strings.
   */
  private static String getJsonStrFromByteMap(Map<String, ByteIterator> map) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(map);
    return JSONValue.toJSONString(stringMap);
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] data, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    JSONObject jsonObject = (JSONObject)JSONValue.parse(new String(data, UTF_8));
    Iterator<String> iterator = jsonObject.keySet().iterator();
    while(iterator.hasNext()) {
      String field = iterator.next();
      String value = jsonObject.get(field).toString();
      if(fields == null || fields.contains(field)) {
        result.put(field, new StringByteIterator(value));
      }
    }
    return result;
  }

  private static class SimpleWatcher implements Watcher {

    public void process(WatchedEvent e) {
      if (e.getType() == Event.EventType.None) {
        return;
      }

      if (e.getState() == Event.KeeperState.SyncConnected) {
        //do nothing
      }
    }
  }
}
