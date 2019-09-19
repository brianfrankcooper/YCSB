/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
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
 * This client provideds a wrapper layer for running the Yahoo Cloud Serving
 * Benchmark (YCSB) against VoltDB. This benchmark runs a synchronous client
 * with a mix of the operations provided below. YCSB is open-source, and may
 * be found at https://github.com/brianfrankcooper/YCSB. The YCSB jar must be
 * in your classpath to compile this client.
 */
package site.ycsb.db.voltdb;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.NoConnectionsException;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.voltdb.sortedvolttable.VoltDBTableSortedMergeWrangler;

/**
 * A client that can be used by YCSB to work with VoltDB.
 */
public class VoltClient4 extends DB {

  private Client mclient;
  private byte[] mworkingData;
  private ByteBuffer mwriteBuf;
  private boolean useScanAll = false;
  private static final Charset UTF8 = Charset.forName("UTF-8");
  
  private Logger logger = LoggerFactory.getLogger(VoltClient4.class);
  
  private YCSBSchemaBuilder ysb =  null;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String servers = props.getProperty("voltdb.servers", "localhost");
    String user = props.getProperty("voltdb.user", "");
    String password = props.getProperty("voltdb.password", "");
    String strLimit = props.getProperty("voltdb.ratelimit");
    String useScanAllParam = props.getProperty("voltdb.scanall", "no");

    if (useScanAllParam.equalsIgnoreCase("YES")) {
      useScanAll = true;
    }

    int ratelimit = strLimit != null ? Integer.parseInt(strLimit) : Integer.MAX_VALUE;
    try {
      mclient = ConnectionHelper.createConnection(servers, user, password, ratelimit);
      
      ysb = StaticHolder.INSTANCE;
      ysb.loadClassesAndDDLIfNeeded(mclient);

    } catch (Exception e) {
      logger.error("Error while creating connection: ", e);
      throw new DBException(e.getMessage());
    }
    mworkingData = new byte[1024 * 1024];
    mwriteBuf = ByteBuffer.wrap(mworkingData);
  }

  /**
   * @return true if we have a live DB connection
   */
  public boolean hasConnection() {

    if (mclient != null && mclient.getConnectedHostList().size() > 0) {
      return true;
    }

    return false;

  }

  @Override
  public void cleanup() throws DBException {
 
    // If VoltDB client exists and has a live connection...
    if (mclient != null && mclient.getConnectedHostList().size() > 0) {

      try {
        mclient.drain();
        mclient.close();
      } catch (NoConnectionsException e) {
        logger.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }

      mclient = null;
    }    
   
    
  }

  @Override
  public Status delete(String keyspace, String key) {
    try {
      ClientResponse response = mclient.callProcedure("STORE.delete", key, keyspace.getBytes(UTF8));
      return response.getStatus() == ClientResponse.SUCCESS ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error while deleting row", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String keyspace, String key, Map<String, ByteIterator> columns) {
    return update(keyspace, key, columns);
  }

  @Override
  public Status read(String keyspace, String key, Set<String> columns, Map<String, ByteIterator> result) {
    try {
      ClientResponse response = mclient.callProcedure("Get", keyspace.getBytes(UTF8), key);
      if (response.getStatus() != ClientResponse.SUCCESS) {
        return Status.ERROR;
      }
      VoltTable table = response.getResults()[0];
      if (table.advanceRow()) {
        unpackRowData(table, columns, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error while GETing row", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String keyspace, String lowerBound, int recordCount, Set<String> columns,
      Vector<HashMap<String, ByteIterator>> result) {

    try {

      if (useScanAll) {

        byte[] ks = keyspace.getBytes(UTF8);
        ClientResponse response = mclient.callProcedure("ScanAll", ks, lowerBound.getBytes(UTF8), recordCount);

        if (response.getStatus() != ClientResponse.SUCCESS) {
          return Status.ERROR;
        }

        result.ensureCapacity(recordCount);

        VoltTable outputTable = response.getResults()[0];
        outputTable.resetRowPosition();
        while (outputTable.advanceRow()) {
          result.add(unpackRowDataHashMap(outputTable, columns));
        }

      } else {

        byte[] ks = keyspace.getBytes(UTF8);
        ClientResponseWithPartitionKey[] response = mclient.callAllPartitionProcedure("Scan", ks,
            lowerBound.getBytes(UTF8), recordCount);

        for (int i = 0; i < response.length; i++) {
          if (response[i].response.getStatus() != ClientResponse.SUCCESS) {
            return Status.ERROR;
          }

        }

        result.ensureCapacity(recordCount);

        VoltDBTableSortedMergeWrangler smw = new VoltDBTableSortedMergeWrangler(response);

        VoltTable outputTable = smw.getSortedTable(1, recordCount);
        outputTable.resetRowPosition();
        while (outputTable.advanceRow()) {
          result.add(unpackRowDataHashMap(outputTable, columns));
        }

      }

      return Status.OK;
    } catch (Exception e) {
      logger.error("Error while calling SCAN", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String keyspace, String key, Map<String, ByteIterator> columns) {
    try {
      ClientResponse response = mclient.callProcedure("Put", keyspace.getBytes(UTF8), key, packRowData(columns));
      return response.getStatus() == ClientResponse.SUCCESS ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error while calling Update", e);
      return Status.ERROR;
    }
  }

  private byte[] packRowData(Map<String, ByteIterator> columns) {
    mwriteBuf.clear();
    mwriteBuf.putInt(columns.size());
    for (String key : columns.keySet()) {
      byte[] k = key.getBytes(UTF8);
      mwriteBuf.putInt(k.length);
      mwriteBuf.put(k);

      ByteIterator v = columns.get(key);
      int len = (int) v.bytesLeft();
      mwriteBuf.putInt(len);
      v.nextBuf(mworkingData, mwriteBuf.position());
      mwriteBuf.position(mwriteBuf.position() + len);
    }

    byte[] data = new byte[mwriteBuf.position()];
    System.arraycopy(mworkingData, 0, data, 0, data.length);
    return data;
  }

  private Map<String, ByteIterator> unpackRowData(VoltTable data, Set<String> fields,
      Map<String, ByteIterator> result) {
    byte[] rowData = data.getVarbinary(0);
    ByteBuffer buf = ByteBuffer.wrap(rowData);
    int nFields = buf.getInt();
    return unpackRowData(rowData, buf, nFields, fields, result);
  }

  private Map<String, ByteIterator> unpackRowData(byte[] rowData, ByteBuffer buf, int nFields, Set<String> fields,
      Map<String, ByteIterator> result) {
    for (int i = 0; i < nFields; i++) {
      int len = buf.getInt();
      int off = buf.position();
      String key = new String(rowData, off, len, UTF8);
      buf.position(off + len);
      len = buf.getInt();
      off = buf.position();
      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(rowData, off, len));
      }
      buf.position(off + len);
    }
    return result;
  }

  private HashMap<String, ByteIterator> unpackRowDataHashMap(VoltTable data, Set<String> fields) {
    byte[] rowData = data.getVarbinary(0);
    ByteBuffer buf = ByteBuffer.wrap(rowData);
    int nFields = buf.getInt();
    int size = fields != null ? Math.min(fields.size(), nFields) : nFields;
    HashMap<String, ByteIterator> res = new HashMap<String, ByteIterator>(size, (float) 1.25);
    return unpackRowDataHashMap(rowData, buf, nFields, fields, res);
  }

  private HashMap<String, ByteIterator> unpackRowDataHashMap(byte[] rowData, ByteBuffer buf, int nFields,
      Set<String> fields, HashMap<String, ByteIterator> result) {
    for (int i = 0; i < nFields; i++) {
      int len = buf.getInt();
      int off = buf.position();
      String key = new String(rowData, off, len, UTF8);
      buf.position(off + len);
      len = buf.getInt();
      off = buf.position();
      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(rowData, off, len));
      }
      buf.position(off + len);
    }
    return result;
  }
  
  private static class StaticHolder {
    static final YCSBSchemaBuilder INSTANCE = new YCSBSchemaBuilder();
  }
  
  

}
