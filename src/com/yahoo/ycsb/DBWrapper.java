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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.security.AccessControlList;
import com.yahoo.ycsb.security.Credential;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
	DB _db;
	Measurements _measurements;

	public DBWrapper(DB db)
	{
		_db=db;
		_measurements=Measurements.getMeasurements();
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
		_db.cleanup();
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
		long st=System.currentTimeMillis();
		int res=_db.read(table,key,fields,result);
		long en=System.currentTimeMillis();
		_measurements.measure("READ",(int)(en-st));
		_measurements.reportReturnCode("READ",res);
		return res;
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
		long st=System.currentTimeMillis();
		int res=_db.scan(table,startkey,recordcount,fields,result);
		long en=System.currentTimeMillis();
		_measurements.measure("SCAN",(int)(en-st));
		_measurements.reportReturnCode("SCAN",res);
		return res;
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
	public int update(String table, String key, HashMap<String,String> values)
	{
		long st=System.currentTimeMillis();
		int res=_db.update(table,key,values);
		long en=System.currentTimeMillis();
		_measurements.measure("UPDATE",(int)(en-st));
		_measurements.reportReturnCode("UPDATE",res);
		return res;
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
	public int insert(String table, String key, HashMap<String,String> values)
	{
		long st=System.currentTimeMillis();
		int res=_db.insert(table,key,values);
		long en=System.currentTimeMillis();
		_measurements.measure("INSERT",(int)(en-st));
		_measurements.reportReturnCode("INSERT",res);
		return res;
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
		long st=System.currentTimeMillis();
		int res=_db.delete(table,key);
		long en=System.currentTimeMillis();
		_measurements.measure("DELETE",(int)(en-st));
		_measurements.reportReturnCode("DELETE",res);
		return res;
	}
	
	/**
	 * Presplit the table
	 *  
	 * @param table The name of the table
	 * @param splitKeys The keys used to split the table
	 * @return Zero on success, a non-zero error code on error. -1 means not supported
	 */
	public int presplit(String table, String[] splitKeys)
	{
		System.out.println("Call presplit from db wrapper");
		long st=System.currentTimeMillis();
		int res = _db.presplit(table, splitKeys);
		long en=System.currentTimeMillis();
		_measurements.measure("SPLIT",(int)(en-st));
		_measurements.reportReturnCode("SPLIT",res);
		return res;
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
		return _db.setFilter(table, filterType, filterOptions);
	}
	
	/**
	 * Clears the filter set on a table.
	 * @param table The name of the table to clear the filter on.
	 */
	public void clearFilters(String table)
	{
		_db.clearFilters(table);
	}

	@Override
	public void setCredential(Credential credential) {
		_db.setCredential(credential);
	}

	@Override
	public void setSchemaAccessControl(List<AccessControlList> acl) {
		_db.setSchemaAccessControl(acl);
	}

	@Override
	public void setOperationAccessControl(List<AccessControlList> acl) {
		_db.setOperationAccessControl(acl);
	}



}
