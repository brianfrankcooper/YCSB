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


import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.io.IOException;
import java.util.*;
//import java.util.HashMap;
//import java.util.Properties;
//import java.util.Set;
//import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.io.Cell;
//import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;


/**
 * HBase client for YCSB framework
 */
public class HBaseClient extends DB
{

	// BFC: Change to fix broken build (with HBase 0.20.6)
	//private static final Configuration config = HBaseConfiguration.create();
	private static final Configuration config = HBaseConfiguration.create();

	public boolean _debug=false;

	public String _table="";
	public HTable _hTable=null;
	public String _columnFamily="";
	public byte _columnFamilyBytes[];
	public boolean _uniqueColumnFamilies=false;
	
	public static final int Ok=0;
	public static final int ServerError=-1;
	public static final int HttpError=-2;
	public static final int NoMatchingRecord=-3;
	public static final int BadFilterError = -4;

	public static final Object tableLock = new Object();

	public FilterBase _scanFilter=null;

	public int bufferSize = 12 * 1024;// unit : Kilobytes




	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		if ( (getProperties().getProperty("debug.hbase")!=null) &&
				(getProperties().getProperty("debug.hbase").compareTo("true")==0) )
		{
			_debug=true;
		}

		_columnFamily = getProperties().getProperty("columnfamily");
		if (_columnFamily == null) 
		{
			System.err.println("Error, must specify a columnfamily for HBase table");
			throw new DBException("No columnfamily specified");
		}
		_columnFamilyBytes = Bytes.toBytes(_columnFamily);

		if ( (getProperties().getProperty("uniquefamilies")!=null) &&
		     (getProperties().getProperty("uniquefamilies").compareTo("true")==0))
		{
				_uniqueColumnFamilies=true;
		}
		
		if (getProperties().getProperty("bufferSize") != null )
		{
			bufferSize = Integer.parseInt(getProperties().getProperty("bufferSize"));
		}

	}





	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		try {
			if (_hTable != null) {
				_hTable.flushCommits();
			}
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	public void getHTable(String table) throws IOException
	{
		synchronized (tableLock) {
			_hTable = new HTable(config, table);
			//2 suggestions from http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
			_hTable.setAutoFlush(false);
			_hTable.setWriteBufferSize(1024*bufferSize);
			System.out.println("Buffer size " + bufferSize * 1024);
			//return hTable;
		}

	}

	/**
	 * Sets a filter for a table. This filter will be applied to all future reads or scans.
	 * Interpretation of the filter is specific to the individual database backend.
	 * A default no-op implementation is provided here so that implementation of this functionality
	 * is optional.
	 * @param table The name of the table for the filter
	 * @param filterType The name of the filter to apply. Interpretation specific to backends.
	 * @param filterOptions Options for the filter. Interpretation specific to backends.
	 * @return Zero on success, a non-zerror error code on error, such as an error in the filter strings.
	 */
	public int setFilter(String table, Filter filterType, String filterOptions)
	{
		RegexStringComparator regexComparator;
		//if this is a "new" table, init HTable object.  Else, use existing one
		if (!_table.equals(table)) {
			_hTable = null;
			try 
			{
				getHTable(table);
				_table = table;
			}
			catch (IOException e) 
			{
				System.err.println("Error accessing HBase table: "+e);
				return ServerError;
			}
		}


		switch (filterType) {
		case FIELD:
			regexComparator = new RegexStringComparator(filterOptions);
			if (regexComparator == null) {
				System.err.println("Bad regex for field filter: " + filterOptions);
				return BadFilterError;
			}
			_scanFilter = new QualifierFilter(CompareOp.EQUAL, regexComparator);
			break;
		case VALUE:
			regexComparator = new RegexStringComparator(filterOptions);
			if (regexComparator == null) {
				System.err.println("Bad regex for value filter: " + filterOptions);
				return BadFilterError;
			}
			_scanFilter = new ValueFilter(CompareOp.EQUAL, regexComparator);
			break;
		case KEY:
			regexComparator = new RegexStringComparator(filterOptions);
			if (regexComparator == null) {
				System.err.println("Bad regex for key filter: " + filterOptions);
				return BadFilterError;
			}
			_scanFilter = new RowFilter(CompareOp.EQUAL, regexComparator);
			break;
		case FIELD_VALUE:
			/* We use the first ':' character as the split between the column name */
			/* and column name. */
			String[] fieldAndFilter = filterOptions.split(":", 2);
			if (fieldAndFilter.length < 2) {
				System.err.println("Bad format for field-Value filter options: " + filterOptions);
				return BadFilterError;
			}
			regexComparator = new RegexStringComparator(fieldAndFilter[1]);
			if (regexComparator == null) {
				System.err.println("Bad regex for field-Value filter: " + fieldAndFilter[1]);
				return BadFilterError;
			}
			_scanFilter = new SingleColumnValueFilter(_columnFamilyBytes, Bytes.toBytes(fieldAndFilter[0]), CompareOp.EQUAL, regexComparator);
			break;
		}

		return 0;
	}

	/**
	 * Clears the filter set on a table.
	 * @param table The name of the table to clear the filter on.
	 */
	public void clearFilters(String table)
	{
		//if this is a "new" table, init HTable object.  Else, use existing one
		if (!_table.equals(table)) {
			_hTable = null;
			try 
			{
				getHTable(table);
				_table = table;
			}
			catch (IOException e) 
			{
				System.err.println("Error accessing HBase table: "+e);
				return ;//ServerError;
			}
		}
		_scanFilter = null;
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
	public int read(String table, String key, Set<String> fields, HashMap<String,String> result)

	{
		//if this is a "new" table, init HTable object.  Else, use existing one
		if (!_table.equals(table)) {
			_hTable = null;
			try 
			{
				getHTable(table);
				_table = table;
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
			if (_debug) {
				System.out.println("Doing read from HBase columnfamily "+_columnFamily);
				System.out.println("Doing read for key: "+key);
			}
			Get g = new Get(Bytes.toBytes(key));
			if ((fields == null) && !_uniqueColumnFamilies) {
				g.addFamily(_columnFamilyBytes);
			} else if (fields != null) {
				for (String field : fields) {
					if (!_uniqueColumnFamilies)
						g.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
					else
						g.addColumn(Bytes.toBytes(field), Bytes.toBytes(field));
				}
			}
			r = _hTable.get(g);
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
					Bytes.toString(kv.getValue()));
			if (_debug) {
				System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
						" is: "+Bytes.toString(kv.getValue()));
			}

		}
		return Ok;
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
	
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,String>> result)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try 
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e) 
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Scan s = new Scan(Bytes.toBytes(startkey));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        s.setCaching(recordcount);

        if (_scanFilter != null) {
        	s.setFilter(_scanFilter);
        }

        //add specified fields or else all fields
        if ((fields == null) && !_uniqueColumnFamilies)
        {
            s.addFamily(_columnFamilyBytes);
        }
        else if (fields != null)
        {
	    for (String field : fields)
            {
		    if (!_uniqueColumnFamilies)
			    s.addColumn(_columnFamilyBytes,Bytes.toBytes(field));
		    else
			    s.addColumn(Bytes.toBytes(field), Bytes.toBytes(field));
            }
        }

        //get results
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String key = Bytes.toString(rr.getRow());

                HashMap<String,String> rowResult = new HashMap<String, String>();

                for (KeyValue kv : rr.raw()) {
                    if (_debug)
			{
			    System.out.format("Scan result: Key='%s', Column='%s:%s', Value='%s'\n", 
					      Bytes.toString(kv.getRow()), 
					      Bytes.toString(kv.getFamily()), 
					      Bytes.toString(kv.getQualifier()), 
					      Bytes.toString(kv.getValue()));
			}

                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      Bytes.toString(kv.getValue()));
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
            if (_debug)
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
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int update(String table, String key, HashMap<String,String> values)
	{
		//if this is a "new" table, init HTable object.  Else, use existing one
		if (!_table.equals(table)) {
			_hTable = null;
			try 
			{
				getHTable(table);
				_table = table;
			}
			catch (IOException e) 
			{
				System.err.println("Error accessing HBase table: "+e);
				return ServerError;
			}
		}


		if (_debug) {
			System.out.println("Setting up put for key: "+key);
		}
		Put p = new Put(Bytes.toBytes(key));
		for (Map.Entry<String, String> entry : values.entrySet())
		{
			if (_debug) {
				System.out.println("Adding field/value " + entry.getKey() + "/"+
						entry.getValue() + " to put request");
			}
			if (_uniqueColumnFamilies)
				p.add(Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
			else
				p.add(_columnFamilyBytes,Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
		}

		try 
		{
			_hTable.put(p);
		}
		catch (IOException e)
		{
			if (_debug) {
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
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
>>>>>>> 1.11
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String,String> values)
	{
		return update(table,key,values);
	}

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key)
	{
		//if this is a "new" table, init HTable object.  Else, use existing one
		if (!_table.equals(table)) {
			_hTable = null;
			try 
			{
				getHTable(table);
				_table = table;
			}
			catch (IOException e) 
			{
				System.err.println("Error accessing HBase table: "+e);
				return ServerError;
			}
		}

		if (_debug) {
			System.out.println("Doing delete for key: "+key);
		}

		Delete d = new Delete(Bytes.toBytes(key));
		try 
		{
			_hTable.delete(d);
		}
		catch (IOException e)
		{
			if (_debug) {
				System.err.println("Error doing delete: "+e);
			}
			return ServerError;
		}

		return Ok;
	}

	/**
	 * Presplit the table
	 *  
	 * @param table The name of the table
	 * @param splitKeys The keys used to split the table
	 * @return Zero on success, a non-zero error code on error. -1 means not supported
	 */
	@Override
	public int presplit(String table, String[] splitKeys) 
	{
		// There are at least two function call for split in Hbase:
		// createTable : sync
		// split	   : async
		HBaseAdmin admin = null;
		System.out.println("Connecting for hbaseadmin now");

		byte[][] keys = new byte[splitKeys.length][];
		for (int i = 0;i < splitKeys.length; i ++)
		{
			keys[i] = splitKeys[i].getBytes();
		}

		try {

			admin= new HBaseAdmin(config);
			if (getProperties().getProperty("createtable") != null) {
				HColumnDescriptor cdesc = new HColumnDescriptor(_columnFamily);
				HTableDescriptor tableDesc = new HTableDescriptor(table);
				tableDesc.addFamily(cdesc);
				System.out.println("Creating the table now.... ");
				admin.createTable(tableDesc, keys);
			} else {
				for (int i = 0;i < splitKeys.length; i ++) {
					System.out.println("Add split with row key" + splitKeys[i].toString());
					admin.split(table.getBytes(), keys[i]);
				}
			}	
		} catch (MasterNotRunningException e1) {
			System.err.println("MasterNotRunningException");
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			System.err.println("ZooKeeperConnectionException");
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("IOException");
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.err.println("InterruptedException");
			e.printStackTrace();
		}


		return 0;
	}

	public static void main(String[] args)
	{
		if (args.length!=3)
		{
			System.out.println("Please specify a threadcount, columnfamily and operation count");
			System.exit(0);
		}

		final int keyspace=10000; //120000000;

		final int threadcount=Integer.parseInt(args[0]);	 

		final String columnfamily=args[1];


		final int opcount=Integer.parseInt(args[2])/threadcount;

		Vector<Thread> allthreads=new Vector<Thread>();

		for (int i=0; i<threadcount; i++)
		{
			Thread t=new Thread() 
			{
				public void run()
				{
					try
					{
						Random random=new Random();

						HBaseClient cli=new HBaseClient();

						Properties props=new Properties();
						props.setProperty("columnfamily",columnfamily);
						props.setProperty("debug","true");
						cli.setProperties(props);

						cli.init();

						//HashMap<String,String> result=new HashMap<String,String>();

						long accum=0;

						for (int i=0; i<opcount; i++)
						{
							int keynum=random.nextInt(keyspace);
							String key="user"+keynum;
							long st=System.currentTimeMillis();
							int rescode;
							/*
                            HashMap hm = new HashMap();
                            hm.put("field1","value1");
                            hm.put("field2","value2");
                            hm.put("field3","value3");
                            rescode=cli.insert("table1",key,hm);
                            HashSet<String> s = new HashSet();
                            s.add("field1");
                            s.add("field2");

                            rescode=cli.read("table1", key, s, result);
                            //rescode=cli.delete("table1",key);
                            rescode=cli.read("table1", key, s, result);
							 */
							HashSet<String> scanFields = new HashSet<String>();
							scanFields.add("field1");
							scanFields.add("field3");
							Vector<HashMap<String,String>> scanResults = new Vector<HashMap<String,String>>();
							rescode = cli.scan("table1","user2",20,null,scanResults);

							long en=System.currentTimeMillis();

							accum+=(en-st);

							if (rescode!=Ok)
							{
								System.out.println("Error "+rescode+" for "+key);
							}

							if (i%1==0)
							{
								System.out.println(i+" operations, average latency: "+(((double)accum)/((double)i)));
							}
						}

						//System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
						//System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			};
			allthreads.add(t);
		}

		long st=System.currentTimeMillis();
		for (Thread t: allthreads)
		{
			t.start();
		}

		for (Thread t: allthreads)
		{
			try
			{
				t.join();
			}
			catch (InterruptedException e)
			{
			}
		}
		long en=System.currentTimeMillis();

		System.out.println("Throughput: "+((1000.0)*(((double)(opcount*threadcount))/((double)(en-st))))+" ops/sec");

	}
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
 */

