/**
 * Copyright (c) 2014 - 2016 YCSB Contributors. All rights reserved.
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
package site.ycsb.db;

import site.ycsb.*;
import org.tarantool.TarantoolConnection16;
import org.tarantool.TarantoolConnection16Impl;
import org.tarantool.TarantoolException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * YCSB binding for <a href="http://tarantool.org/">Tarantool</a>.
 */
public class TarantoolClient extends DB {
  private static final Logger LOGGER = Logger.getLogger(TarantoolClient.class.getName());

  private static final String HOST_PROPERTY = "tarantool.host";
  private static final String PORT_PROPERTY = "tarantool.port";
  private static final String SPACE_PROPERTY = "tarantool.space";
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3301";
  private static final String DEFAULT_SPACE = "1024";

  private TarantoolConnection16 connection;
  private int spaceNo;

  public void init() throws DBException {
    Properties props = getProperties();

    int port = Integer.parseInt(props.getProperty(PORT_PROPERTY, DEFAULT_PORT));
    String host = props.getProperty(HOST_PROPERTY, DEFAULT_HOST);
    spaceNo = Integer.parseInt(props.getProperty(SPACE_PROPERTY, DEFAULT_SPACE));

    try {
      this.connection = new TarantoolConnection16Impl(host, port);
    } catch (Exception exc) {
      throw new DBException("Can't initialize Tarantool connection", exc);
    }
  }

  public void cleanup() throws DBException {
    this.connection.close();
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return replace(key, values, "Can't insert element");
  }

  private HashMap<String, ByteIterator> tupleConvertFilter(List<String> input, Set<String> fields) {
    HashMap<String, ByteIterator> result = new HashMap<>();
    if (input == null) {
      return result;
    }
    for (int i = 1; i < input.toArray().length; i += 2) {
      if (fields == null || fields.contains(input.get(i))) {
        result.put(input.get(i), new StringByteIterator(input.get(i + 1)));
      }
    }
    return result;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      List<String> response = this.connection.select(this.spaceNo, 0, Arrays.asList(key), 0, 1, 0);
      result = tupleConvertFilter(response, fields);
      return Status.OK;
    } catch (TarantoolException exc) {
      LOGGER.log(Level.SEVERE, "Can't select element", exc);
      return Status.ERROR;
    } catch (NullPointerException exc) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey,
                     int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    List<List<String>> response;
    try {
      response = this.connection.select(this.spaceNo, 0, Arrays.asList(startkey), 0, recordcount, 6);
    } catch (TarantoolException exc) {
      LOGGER.log(Level.SEVERE, "Can't select range elements", exc);
      return Status.ERROR;
    } catch (NullPointerException exc) {
      return Status.ERROR;
    }
    for (List<String> i : response) {
      HashMap<String, ByteIterator> temp = tupleConvertFilter(i, fields);
      if (!temp.isEmpty()) {
        result.add((HashMap<String, ByteIterator>) temp.clone());
      }
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      this.connection.delete(this.spaceNo, Collections.singletonList(key));
    } catch (TarantoolException exc) {
      LOGGER.log(Level.SEVERE, "Can't delete element", exc);
      return Status.ERROR;
    } catch (NullPointerException e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return replace(key, values, "Can't replace element");
  }

  private Status replace(String key, Map<String, ByteIterator> values, String exceptionDescription) {
    int j = 0;
    String[] tuple = new String[1 + 2 * values.size()];
    tuple[0] = key;
    for (Map.Entry<String, ByteIterator> i : values.entrySet()) {
      tuple[j + 1] = i.getKey();
      tuple[j + 2] = i.getValue().toString();
      j += 2;
    }
    try {
      this.connection.replace(this.spaceNo, tuple);
    } catch (TarantoolException exc) {
      LOGGER.log(Level.SEVERE, exceptionDescription, exc);
      return Status.ERROR;
    }
    return Status.OK;

  }
}
