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

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.List;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 * 
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whether
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your 
 * target application.  For the sake of comparison between experiments we also 
 * recommend you explain the semantics you chose when presenting performance results.
 */
public abstract class DB
{
	/**
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
	}

    /**
     * Reinitialize this DB.
     */
    public void reinit() throws DBException, InstantiationException, IllegalAccessException {
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
	public abstract int readOne(String table, String key, String field, Map<String,ByteIterator> result);

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public abstract int readAll(String table, String key, Map<String,ByteIterator> result);

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param result A List of Maps, where each Map is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
    public abstract int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result);

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
    public abstract int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result);

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param value The value to update in the key record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public abstract int updateOne(String table, String key, String field, ByteIterator value);
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public abstract int updateAll(String table, String key, Map<String,ByteIterator> values);

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
	 * record key.
	 *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A Map of field/value pairs to insert in the record
     * @return Zero on success, a negative non-zero error code on error, and a positive non-zero error code on retry. See this class's description for a discussion of error codes.
	 */
	public abstract int insert(String table, String key, Map<String, ByteIterator> values);

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int delete(String table, String key);
}
