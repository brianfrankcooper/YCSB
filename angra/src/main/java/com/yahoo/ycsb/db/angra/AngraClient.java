/**
 * Copyright (c) 2016 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db.angra;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.StringWriter;
import java.io.Writer;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A class that wraps the Angra-DB to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>angra.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>angra.port=1234</b> The port from one server.</li>
 * <li><b>angra.schema=ycsb</b> The schema name to use.</li>
 * </ul>
 */
public class AngraClient extends DB {

  private static final String SEPARATOR = "-";

  private String host;
  private String port;
  private String schema;
  private Driver driver;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    host = props.getProperty("angra.host", "127.0.0.1");
    port = props.getProperty("angra.port", "1234");
    schema = props.getProperty("angra.schema", "ycsb");

    driver= new Driver(host,port);
    driver.createDatabase(schema);
    driver.connectToDatabase(schema);
  }

  @Override
  public Status read(final String table, final String key, Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      String docId = formatId(table, key);
      String resp = driver.lookup(docId);
      if resp.substring(0,1).equals("\""){
        return Status.OK;
      }else{
        return Status.NOT_FOUND;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      String docId = formatId(table, key);
      String resp = driver.update(docId, value.toString());
      if (resp.equals("ok")){
        return Status.OK;
      }else{
        return Status.NOT_FOUND;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      String docId = formatId(table, key);
      String recKey = driver.save_key(docId, value.toString())
      if (recKey.equals(docId)){
        return Status.OK;
      }else{
        return Status.NOT_FOUND;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      String docId = formatId(table, key);
      String resp = driver.delete(docId);
      if (resp.equals("ok")){
        return Status.OK;
      }else{
        return Status.NOT_FOUND;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    try {
      // TODO
      return Status.OK;
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that to be used with Angra-DB.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + SEPARATOR + key;
  }
