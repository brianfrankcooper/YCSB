/**
 * Copyright (c) 2022 YCSB contributors. All rights reserved.
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

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import java.io.IOException;

import io.daos.BufferAllocator;
import io.daos.DaosObjClassHint;
import io.daos.DaosObjectClass;
import io.daos.DaosObjectType;

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectException;
import io.daos.obj.DaosObjectId;
import io.daos.obj.IODataDescSync;
import io.daos.obj.IOKeyDesc;

import io.netty.buffer.ByteBuf;

/**
 * YCSB binding for <a href="https://docs.daos.io/v2.0/">DAOS</a>.
 */
public class DaosClient extends site.ycsb.DB {

  private static DaosObject tblObject = null;
  private static DaosObjClient objClient = null;
  private static int refCnt = 0;

  private static final int RECORD_SIZE = 512;
  private int recordSize = RECORD_SIZE;

  @Override
  public void init() throws DBException {
    synchronized(DaosClient.class) {
      if (refCnt++ != 0) {
        return;
      }
      Properties props = getProperties();
      String poolId = props.getProperty("daos.pool");
      String contId = props.getProperty("daos.cont");
      String fieldlength = props.getProperty("fieldlength");
      String readallfields = props.getProperty("readallfields");
      String readallfieldsbyname = props.getProperty("readallfieldsbyname");
      boolean isReadAllFields = true;
      boolean isReadAllFieldsByName = false;

      if (fieldlength != null) {
        recordSize = Integer.parseInt(fieldlength);
      }
      System.out.println("DAOS binding Record size set to " + recordSize);

      if (readallfields != null) {
        isReadAllFields = Boolean.parseBoolean(readallfields);
      }
      if (readallfieldsbyname != null) {
        isReadAllFieldsByName = Boolean.parseBoolean(readallfieldsbyname);
      }
      if (isReadAllFields && !isReadAllFieldsByName) {
        System.out.println("DAOS binding detected property readallfields=true. " +
                           "For optimal performance set readallfieldsbyname=true aswell");
      }

      DaosObjectId oid = new DaosObjectId(0, 2022);
      try {
        objClient = new DaosObjClient.DaosObjClientBuilder().poolId(poolId).containerId(contId).build();
        long cptr = objClient.getContPtr();
        oid.encode(cptr, DaosObjectType.DAOS_OT_MULTI_LEXICAL, DaosObjectClass.OC_UNKNOWN,
                   DaosObjClassHint.DAOS_OCH_SHD_MAX, 0);
        tblObject =  objClient.getObject(oid);
        tblObject.open();
      } catch (IOException e){
        refCnt--;
        throw new DBException(String.format("Error while creating DAOS Object "), e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    synchronized(DaosClient.class) {
      if (--refCnt != 0) {
        return;
      }
      try {
        tblObject.close();
        objClient.close();
      } catch (IOException e) {
        System.err.println("Error during cleanup : " + e);
      }
    }
  }

  private static List<String> listAkeys(DaosObject object, String dkey) throws IOException {
    IOKeyDesc keyDesc = null;
    List<String> list = new ArrayList<>();
    try {
      keyDesc = object.createKD(dkey);
      while (!keyDesc.reachEnd()) {
        keyDesc.continueList();
        list.addAll(object.listAkeys(keyDesc));
      }
      return list;
    } finally {
      if (keyDesc != null) {
        keyDesc.release();
      }
    }
  }


  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    IODataDescSync desc = null;
    Status ret = Status.OK;
    try {
      desc = tblObject.createDataDescForFetch(key, IODataDescSync.IodType.SINGLE, recordSize);
      if (fields == null) {
        List<String> akeys = listAkeys(tblObject, key);
        if (akeys.isEmpty()) {
          System.err.println("Error while reading key " + key + ": " + "Record is emty");
          return Status.ERROR;
        }
        for (String k : akeys) {
          desc.addEntryForFetch(k, 0, recordSize);
        }
      } else {
        for (String k : fields) {
          desc.addEntryForFetch(k, 0, recordSize);
        }
      }
      tblObject.fetch(desc);
      int nEntries = desc.getNbrOfEntries();
      int tSz = 0;
      for (int i = 0; i< nEntries; i++) {
        int sz = desc.getEntry(i).getActualRecSize();
        ByteBuf b = desc.getEntry(i).getFetchedData();
        byte[] dst = new byte[sz];
        b.readBytes(dst, 0, sz);
        ByteArrayByteIterator bArr = new ByteArrayByteIterator(dst);
        result.put(desc.getEntry(i).getKey(), bArr);
        tSz += sz;
      }
      if (tSz == 0) {
        System.err.println("Record with key " + key + " is empty");
        ret = Status.ERROR;
      }
    } catch (IOException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      ret = Status.ERROR;
    } finally {
      if (desc != null) {
        desc.release(true);
      }
    }

    return ret;
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, Map<String, ByteIterator> values) {
    IODataDescSync desc = null;
    Status ret = Status.OK;
    try {
      desc = tblObject.createDataDescForUpdate(key, IODataDescSync.IodType.SINGLE, recordSize);
      for (String k : values.keySet()) {
        byte[] b = values.get(k).toArray();
        ByteBuf databuf = BufferAllocator.directNettyBuf(recordSize).writeBytes(b);
        databuf.writeZero(recordSize-b.length);
        desc.addEntryForUpdate(k, 0, databuf);
      }
      tblObject.update(desc);
    } catch (IOException e) {
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    } finally {
      if (desc != null) {
        desc.release(false);
      }
    }
    return ret;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      List dkeys = new ArrayList<String>();
      dkeys.add(key);
      tblObject.punchDkeys(dkeys);
      return Status.OK;
    } catch (DaosObjectException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
