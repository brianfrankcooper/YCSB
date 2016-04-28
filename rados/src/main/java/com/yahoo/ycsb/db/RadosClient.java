/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.ceph.rados.Rados;
import com.ceph.rados.IoCTX;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.ReadOp;
import com.ceph.rados.ReadOp.ReadResult;
import com.ceph.rados.exceptions.RadosException;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.json.JSONObject;

/**
 * YCSB binding for <a href="http://ceph.org/">RADOS of Ceph</a>.
 *
 * See {@code rados/README.md} for details.
 */
public class RadosClient extends DB {

  private Rados rados;
  private IoCTX ioctx;

  public static final String CONFIG_FILE_PROPERTY = "rados.configfile";
  public static final String CONFIG_FILE_DEFAULT = "/etc/ceph/ceph.conf";
  public static final String ID_PROPERTY = "rados.id";
  public static final String ID_DEFAULT = "admin";
  public static final String POOL_PROPERTY = "rados.pool";
  public static final String POOL_DEFAULT = "data";

  public void init() throws DBException {
    Properties props = getProperties();

    String configfile = props.getProperty(CONFIG_FILE_PROPERTY);
    if (configfile == null) {
      configfile = CONFIG_FILE_DEFAULT;
    }

    String id = props.getProperty(ID_PROPERTY);
    if (id == null) {
      id = ID_DEFAULT;
    }

    String pool = props.getProperty(POOL_PROPERTY);
    if (pool == null) {
      pool = POOL_DEFAULT;
    }

    rados = new Rados(id);
    try {
      rados.confReadFile(new File(configfile));
      rados.connect();
      ioctx = rados.ioCtxCreate(pool);
    } catch (RadosException e) {
      throw new DBException(e.getMessage() + ": " + e.getReturnValue());
    }
  }

  public void cleanup() throws DBException {
    rados.shutDown();
    rados.ioCtxDestroy(ioctx);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    byte[] buffer;

    try {
      RadosObjectInfo info = ioctx.stat(key);
      buffer = new byte[(int)info.getSize()];

      ReadOp rop = ioctx.readOpCreate();
      ReadResult readResult = rop.queueRead(0, info.getSize());
      // TODO: more size than byte length possible;
      // rop.operate(key, Rados.OPERATION_NOFLAG); // for rados-java 0.3.0
      rop.operate(key, 0);
      // readResult.raiseExceptionOnError("Error ReadOP(%d)", readResult.getRVal()); // for rados-java 0.3.0
      if (readResult.getRVal() < 0) {
        throw new RadosException("Error ReadOP", readResult.getRVal());
      }
      if (info.getSize() != readResult.getBytesRead()) {
        return new Status("ERROR", "Error the object size read");
      }
      readResult.getBuffer().get(buffer);
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }

    JSONObject json = new JSONObject(new String(buffer, java.nio.charset.StandardCharsets.UTF_8));
    Set<String> fieldsToReturn = (fields == null ? json.keySet() : fields);

    for (String name : fieldsToReturn) {
      result.put(name, new StringByteIterator(json.getString(name)));
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    JSONObject json = new JSONObject();
    for (final Entry<String, ByteIterator> e : values.entrySet()) {
      json.put(e.getKey(), e.getValue().toString());
    }

    try {
      ioctx.write(key, json.toString());
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      ioctx.remove(key);
    } catch (RadosException e) {
      return new Status("ERROR-" + e.getReturnValue(), e.getMessage());
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    Status rtn = delete(table, key);
    if (rtn.equals(Status.OK)) {
      return insert(table, key, values);
    }
    return rtn;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
