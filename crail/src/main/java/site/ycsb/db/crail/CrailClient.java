/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
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

package site.ycsb.db.crail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailStore;
import org.apache.crail.CrailKeyValue;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.CrailConfiguration;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Crail binding for <a href="http://crail.apache.org/">Crail</a>.
 */
public class CrailClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(CrailClient.class);

  private CrailStore client;
  private long startTime;
  private long endTime;
  private String usertable;
  private boolean enumerateKeys;

  @Override
  public void init() throws DBException {
    super.init();
    try {
      CrailConfiguration crailConf = new CrailConfiguration();
      this.client = CrailStore.newInstance(crailConf);

      usertable = getProperties().getProperty("table", "usertable");
      enumerateKeys = Boolean.parseBoolean(getProperties().getProperty("crail.enumeratekeys", "false"));

      if (client.lookup(usertable).get() == null) {
        client.create(usertable, CrailNodeType.TABLE, CrailStorageClass.DEFAULT, 
          CrailLocationClass.DEFAULT, true).get().syncDir();
      }
      this.startTime = System.nanoTime();
    } catch(Exception e){
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.endTime = System.nanoTime();
      long runTime = (endTime - startTime) / 1000000;
      client.close();
    } catch(Exception e){
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      String path = table + "/" + key;
      CrailKeyValue file = client.lookup(path).get().asKeyValue();
      CrailBufferedInputStream stream = file.getBufferedInputStream(1024);
      while(stream.available() < Integer.BYTES){
        assert true;
      }
      int fieldKeyLength = stream.readInt();
      while(stream.available() < fieldKeyLength){
        assert true;
      }
      byte[] fieldKey = new byte[fieldKeyLength];
      int res = stream.read(fieldKey);
      if (res != fieldKey.length){
        stream.close();
        return Status.ERROR;
      }
      while(stream.available() < Integer.BYTES){
        assert true;
      }
      int fieldValueLength = stream.readInt();
      while(stream.available() < fieldValueLength){
        assert true;
      }
      byte[] fieldValue = new byte[fieldValueLength];
      res = stream.read(fieldValue);
      if (res != fieldValue.length){
        stream.close();
        return Status.ERROR;
      }
      result.put(new String(fieldKey), new ByteArrayByteIterator(fieldValue));

      stream.close();
      return Status.OK;
    } catch(Exception e){
      LOG.error("Error during read, table " + table + ", key " + key + ", exception " + e.getMessage());
      return new Status("read error", "reading exception");
    }
  }

  @Override
  public Status scan(String table, String startKey, int recordCount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      String path = table + "/" + key;
      CrailKeyValue file = client.create(path, CrailNodeType.KEYVALUE, CrailStorageClass.DEFAULT, 
          CrailLocationClass.DEFAULT, enumerateKeys).get().asKeyValue();
      CrailBufferedOutputStream stream = file.getBufferedOutputStream(1024);
      for (Entry<String, ByteIterator> entry : values.entrySet()){
        byte[] fieldKey = entry.getKey().getBytes();
        int fieldKeyLength = fieldKey.length;
        byte[] fieldValue = entry.getValue().toArray();
        int fieldValueLength = fieldValue.length;
        stream.writeInt(fieldKeyLength);
        stream.write(fieldKey);
        stream.writeInt(fieldValueLength);
        stream.write(fieldValue);
      }
      file.syncDir();
      stream.close();
    } catch(Exception e){
      LOG.error("Error during insert, table " + table + ", key " + key + ", exception " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      String path = table + "/" + key;
      client.delete(path, false).get().syncDir();
    } catch(Exception e){
      LOG.error("Error during delete, table " + table + ", key " + key + ", exception " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }
}
