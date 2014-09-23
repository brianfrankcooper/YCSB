/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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


import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * HBase client for YCSB framework
 *
 * hbase.durability = {async_wal,fsync_wal,skip_wal,sync_wal,use_default}
 */
public class HBaseClient extends com.yahoo.ycsb.DB
{
    private static final Configuration config = HBaseConfiguration.create();

    private boolean debug =false;

    private String table ="";
    private HTable hTable =null;
    private Durability durability;
    private String columnFamily ="";
    private byte columnFamilyBytes[];

    private static final int Ok=0;
    private static final int ServerError=-1;
    private static final int HttpError=-2;
    private static final int NoMatchingRecord=-3;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException
    {
        debug = Boolean.parseBoolean("debug");

        durability = Durability.valueOf(getProperties().getProperty("hbase.durability", "FSYNC_WAL").toUpperCase());

        columnFamily = getProperties().getProperty("columnfamily");
        if (columnFamily == null)
        {
            System.err.println("Error, must specify a columnfamily for HBase table");
            throw new DBException("No columnfamily specified");
        }
      columnFamilyBytes = Bytes.toBytes(columnFamily);
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        if (hTable == null)
            return;
        try {
            hTable.close();
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    public void initHTable(String table) throws IOException
    {
        hTable = new HTable(config, table);
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int readOne(String table, String key, String field, Map<String,ByteIterator> result)
    {
        Get g = new Get(Bytes.toBytes(key));
        g.addColumn(columnFamilyBytes, Bytes.toBytes(field));

        return read(table, key, result, g);
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int readAll(String table, String key, Map<String,ByteIterator> result)
    {
        Get g = new Get(Bytes.toBytes(key));
        g.addFamily(columnFamilyBytes);

        return read(table, key, result, g);
    }

    public int read(String table, String key, Map<String, ByteIterator> result, Get g)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!this.table.equals(table)) {
            hTable = null;
            try
            {
                initHTable(table);
                this.table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Result r = null;
        try
        {
        if (debug) {
        System.out.println("Doing read from HBase columnfamily "+ columnFamily);
        System.out.println("Doing read for key: "+key);
        }
            r = hTable.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: "+e);
            return ServerError;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...need to understand HBase concurrency model better
            return ServerError;
        }

  for (KeyValue kv : r.raw()) {
    result.put(
        Bytes.toString(kv.getQualifier()),
        new ByteArrayByteIterator(kv.getValue()));
    if (debug) {
      System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
          " is: "+Bytes.toString(kv.getValue()));
    }

  }
    return Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result)
    {
        Scan s = new Scan(Bytes.toBytes(startkey));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        s.setCaching(recordcount);
        s.addFamily(columnFamilyBytes);

        return scan(table, recordcount, result, s);
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field The field to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    @Override
    public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result)
    {
        Scan s = new Scan(Bytes.toBytes(startkey));
        s.setCaching(recordcount);
        s.addFamily(columnFamilyBytes);
        s.addColumn(columnFamilyBytes,Bytes.toBytes(field));

        return scan(table, recordcount, result, s);
    }

    public int scan(String table, int recordcount, List<Map<String, ByteIterator>> result, Scan s)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!this.table.equals(table)) {
            hTable = null;
            try
            {
                initHTable(table);
                this.table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        //get results
        ResultScanner scanner = null;
        try {
            scanner = hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String key = Bytes.toString(rr.getRow());
                if (debug)
                {
                    System.out.println("Got scan result for key: "+key);
                }

                HashMap<String,ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      new ByteArrayByteIterator(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= recordcount) //if hit recordcount, bail out
                {
                    break;
                }
            } //done with row

        }

        catch (IOException e) {
            if (debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return ServerError;
        }

        finally {
            scanner.close();
        }

        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param value The value to update in the key record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int updateOne(String table, String key, String field, ByteIterator value)
    {
        Put p = new Put(Bytes.toBytes(key));
        p.setDurability(durability);
        if (debug) {
            System.out.println("Adding field/value " + field + "/"+ value + " to put request");
        }
        p.add(columnFamilyBytes,Bytes.toBytes(field),value.toArray());

        return update(table, key, p);
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int updateAll(String table, String key, Map<String,ByteIterator> values)
    {
        Put p = new Put(Bytes.toBytes(key));
        p.setDurability(durability);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet())
        {
            if (debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                        entry.getValue() + " to put request");
            }
            p.add(columnFamilyBytes,Bytes.toBytes(entry.getKey()),entry.getValue().toArray());
        }

        return update(table, key, p);
    }

    public int update(String table, String key, Put p)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!this.table.equals(table)) {
            hTable = null;
            try
            {
                initHTable(table);
                this.table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }


        if (debug) {
            System.out.println("Setting up put for key: "+key);
        }

        try
        {
            hTable.put(p);
        }
        catch (IOException e)
        {
            if (debug) {
                System.err.println("Error doing put: "+e);
            }
            return ServerError;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...hope this is rare
            return ServerError;
        }

        return Ok;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values)
    {
        return updateAll(table,key,values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int delete(String table, String key)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!this.table.equals(table)) {
            hTable = null;
            try
            {
                initHTable(table);
                this.table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        if (debug) {
            System.out.println("Doing delete for key: "+key);
        }

        Delete d = new Delete(Bytes.toBytes(key));
        try
        {
            hTable.delete(d);
        }
        catch (IOException e)
        {
            if (debug) {
                System.err.println("Error doing delete: "+e);
            }
            return ServerError;
        }

        return Ok;
    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/

