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

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
    private static final String READ_RETRY_PROPERTY = "readretrycount";
    private static final String UPDATE_RETRY_PROPERTY = "updateretrycount";
    private static final String INSERT_RETRY_PROPERTY = "insertretrycount";
    private static final String RETRY_DELAY = "retrydelay";

    int _readretrycount;
    int _updateretrycount;
    int _insertretrycount;
    int _retrydelay;

	DB _db;
	Measurements _measurements;

    private interface DBOperation
    {
        String getName();
        int getRetryCount();
        int run();
    }

	public DBWrapper(DB db, Properties p)
	{
		_db=db;
		_measurements=Measurements.getMeasurements();

        _readretrycount = Integer.parseInt(p.getProperty(READ_RETRY_PROPERTY, "0"));
        _updateretrycount = Integer.parseInt(p.getProperty(UPDATE_RETRY_PROPERTY, "0"));
        _insertretrycount = Integer.parseInt(p.getProperty(INSERT_RETRY_PROPERTY, "0"));
        _retrydelay = Integer.parseInt(p.getProperty(RETRY_DELAY, "0"));
	}

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		_db.init();
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
        long st=System.nanoTime();
        _db.cleanup();
        long en=System.nanoTime();
        _measurements.measure("CLEANUP", (int)((en-st)/1000));
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
    public int read(final String table, final String key, final Set<String> fields, final HashMap<String, ByteIterator> result)
    {
        return operation(new DBOperation()
        {
            public String getName()
            {
                return "READ";
            }

            public int getRetryCount()
            {
                return _readretrycount;
            }

            public int run()
            {
                return _db.read(table, key, fields, result);
            }
        });
    }

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int scan(final String table, final String startkey, final int recordcount, final Set<String> fields, final Vector<HashMap<String,ByteIterator>> result)
	{
        return operation(new DBOperation()
        {
            public String getName()
            {
                return "SCAN";
            }

            public int getRetryCount()
            {
                return 0;
            }

            public int run()
            {
                return _db.scan(table,startkey,recordcount,fields,result);
            }
        });
	}
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
    public int update(final String table, final String key, final HashMap<String, ByteIterator> values)
    {
        return operation(new DBOperation()
        {
            public String getName()
            {
                return "UPDATE";
            }

            public int getRetryCount()
            {
                return _updateretrycount;
            }

            public int run()
            {
                return _db.update(table, key, values);
            }
        });
    }

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
    public int insert(final String table, final String key, final HashMap<String, ByteIterator> values)
    {
        return operation(new DBOperation()
        {
            public String getName()
            {
                return "INSERT";
            }

            public int getRetryCount()
            {
                return _insertretrycount;
            }

            public int run()
            {
                return _db.insert(table, key, values);
            }
        });
    }

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
    public int delete(final String table, final String key)
    {
        return operation(new DBOperation()
        {
            public String getName()
            {
                return "DELETE";
            }

            public int getRetryCount()
            {
                return 0;
            }

            public int run()
            {
                return _db.delete(table, key);
            }
        });
    }

    private int operation(DBOperation op)
    {
        long st = System.nanoTime();
        int res = op.run();
        int retrycount;
        for (retrycount = 0; res != 0 && retrycount < op.getRetryCount(); retrycount++)
        {
            delay(_retrydelay);
            res = op.run();
        }
        long en = System.nanoTime();
        _measurements.measure(op.getName(), (int) ((en - st) / 1000));
        _measurements.reportRetryCount(op.getName(), retrycount);
        _measurements.reportReturnCode(op.getName(), res);
        return res;
    }

    private void delay(int ms)
    {
        if(ms <= 0)
        {
            return;
        }
        try
        {
            Thread.sleep(ms);
        } catch (InterruptedException e)
        {
            //do nothing
        }
    }
}
