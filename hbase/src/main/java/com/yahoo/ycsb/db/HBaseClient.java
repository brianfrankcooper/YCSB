/*
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
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * HBase client for YCSB framework
 */
public class HBaseClient extends com.yahoo.ycsb.DB
{
    private final Configuration config;
    private final HConnection connection;
    private final ThreadPoolExecutor executor;

    public boolean debug = false;
    public String columnFamily = "";
    public byte columnFamilyBytes[];

    public static final int Ok = 0;
    public static final int ServerError = -1;
    public static final int HttpError = -2;
    public static final int NoMatchingRecord = -3;

    public HBaseClient() throws IOException
    {
        super();
        config = HBaseConfiguration.create();
        // Disable Nagle on the client, hope we've done the same on the server
        config.setBoolean("hbase.ipc.client.tcpnodelay", true);
        connection = HConnectionManager.createConnection(config);
        int coreThreads = Runtime.getRuntime().availableProcessors() * 8;
        // Maintain a useful minimum of core threads even on wimpy hosts
        if (coreThreads < 32)
        {
            coreThreads = 32;
        }
        int maxThreads = coreThreads * 8;
        this.executor = new ThreadPoolExecutor(coreThreads,
                                               maxThreads,
                                               // Time out threads after 60 seconds of inactivity
                                               60, TimeUnit.SECONDS,
                                               // Queue up to N tasks per worker threads (N=100 by default)
                                               new LinkedBlockingQueue<Runnable>(maxThreads * config.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                                                                                                            HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS)),
                                               // Create daemon threads
                                               new ThreadFactory()
                                               {
                                                   public Thread newThread(Runnable r)
                                                   {
                                                       Thread t = new Thread(r);
                                                       t.setDaemon(true);
                                                       return t;
                                                   }
                                               });
        // Allow for the core thread pool to shrink with inactivity
        this.executor.allowCoreThreadTimeOut(true);
        // This is YCSB, we should prep for drag racing
        this.executor.prestartAllCoreThreads();
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException
    {
        if ((getProperties().getProperty("debug") != null) &&
            (getProperties().getProperty("debug").compareTo("true") == 0))
        {
            debug = true;
        }
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
        Measurements _measurements = Measurements.getMeasurements();
        long st = System.nanoTime();
        try
        {
            connection.close();
        }
        catch (IOException e)
        {
            throw new DBException(e);
        }
        finally
        {
            long en = System.nanoTime();
            _measurements.measure("UPDATE", (int) ((en - st) / 1000));
        }
    }

    private HTableInterface getHTable(String table) throws IOException
    {
        HTableInterface t = connection.getTable(table, executor);
        // This is currently a no-op. We will get a new HTI for every DB op
        // requested by core. This is "lightweight" according to HBase docs
        // since we are managing our own connections as is the new preferred
        // way of doing things. The previous HBase YCSB driver implementation
        // cheated significantly by holding on to one HTable instance per
        // thread such that writes would go into the write buffer and not be
        // interleaved with other ops as YCSB expects. YCSB wants to measure
        // the round trip of every op, not the non-cost of local caching. Now
        // that we close() the HTI after every operation, triggering a flush,
        // we act as YCSB intended. We should still set auto flush to false in
        // case YCSB evolves where writes could be batched.
        t.setAutoFlushTo(false);
        return t;
    }

    private void putHTable(HTableInterface t)
    {
        if (t != null) try
        {
            t.close();
        }
        catch (IOException e)
        {
            // ignore
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored
     * in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        HTableInterface t = null;
        Result r = null;
        try
        {
            if (debug)
            {
                System.out.println("Doing read for key " + key);
            }
            Get g = new Get(Bytes.toBytes(key));
            if (fields == null)
            {
                g.addFamily(columnFamilyBytes);
            }
            else
            {
                for (String field : fields)
                {
                    g.addColumn(columnFamilyBytes, Bytes.toBytes(field));
                }
            }
            t = getHTable(table);
            r = t.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: " + e);
            return ServerError;
        }
        finally
        {
            putHTable(t);
        }

        Cell[] cells = r.rawCells();
        if (cells != null)
        {
            for (int i = 0; i < cells.length; i++)
            {
                result.put(new String(cells[i].getQualifierArray(), cells[i].getQualifierOffset(),
                                      cells[i].getQualifierLength()),
                           new ByteArrayByteIterator(cells[i].getValueArray(), cells[i].getValueOffset(),
                                                     cells[i].getValueLength()));
            }
        }
        if (debug)
        {
            System.out.println("Completed read for key " + key + ", " + result.size() +
                               " cells returned");
        }

        return result.isEmpty() ? NoMatchingRecord : Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the
     * result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result)
    {
        Scan s = new Scan(Bytes.toBytes(startkey));
        // Assume recordcount is small enough to bring back in one call
        s.setCaching(recordcount);

        // Add specified fields or else all fields
        if (fields == null)
        {
            s.addFamily(columnFamilyBytes);
        }
        else
        {
            for (String field : fields)
            {
                s.addColumn(columnFamilyBytes, Bytes.toBytes(field));
            }
        }

        // get results
        HTableInterface t = null;
        ResultScanner scanner = null;
        try
        {
            t = getHTable(table);
            scanner = t.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                // get row key
                if (debug)
                {
                    String key = Bytes.toString(rr.getRow());
                    System.out.println("Got scan result for key: " + key);
                }
                // add rowResult to result vector
                HashMap<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>();
                Cell[] cells = rr.rawCells();
                if (cells != null)
                {
                    for (int i = 0; i < cells.length; i++)
                    {
                        rowResult.put(new String(cells[i].getQualifierArray(), cells[i].getQualifierOffset(), cells[i].getQualifierLength()),
                                      new ByteArrayByteIterator(cells[i].getValueArray(), cells[i].getValueOffset(), cells[i].getValueLength()));
                    }
                }
                result.add(rowResult);
                numResults++;
                // if hit recordcount, bail out
                if (numResults >= recordcount)
                {
                    break;
                }
            }
        }
        catch (IOException e)
        {
            if (debug)
            {
                System.out.println("Error in getting/parsing scan result: " + e);
            }
            return ServerError;
        }
        finally
        {
            if (scanner != null)
            {
                scanner.close();
            }
            putHTable(t);
        }

        return result.isEmpty() ? NoMatchingRecord : Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values
     * HashMap will be written into the record with the specified record key, overwriting
     * any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        if (debug)
        {
            System.out.println("Setting up put for key: " + key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> e : values.entrySet())
        {
            if (debug)
            {
                System.out.println("Adding field/value " + e.getKey() + "/" +
                                   e.getValue() + " to put request");
            }
            p.add(columnFamilyBytes, Bytes.toBytes(e.getKey()), e.getValue().toArray());
        }
        HTableInterface t = null;
        try
        {
            t = getHTable(table);
            t.put(p);
        }
        catch (IOException e)
        {
            if (debug)
            {
                System.err.println("Error doing put: " + e);
            }
            return ServerError;
        }
        finally
        {
            putHTable(t);
        }
        return Ok;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap
     * will be written into the record with the specified record key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        return update(table, key, values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        if (debug)
        {
            System.out.println("Doing delete for key: " + key);
        }
        HTableInterface t = null;
        try
        {
            t = getHTable(table);
            t.delete(new Delete(Bytes.toBytes(key)));
        }
        catch (IOException e)
        {
            if (debug)
            {
                System.err.println("Error doing delete: " + e);
            }
            return ServerError;
        }
        finally
        {
            putHTable(t);
        }
        return Ok;
    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/