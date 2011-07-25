/**
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

import com.yahoo.ycsb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;
import java.nio.ByteBuffer;

import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.http.util.Constants;
import com.basho.riak.pbc.BucketProperties;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.pbc.RiakObject;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException; 

import com.google.protobuf.ByteString;

/**
 * @author yourabi
 **/
public class RiakClient extends DB {
	static Random random = new Random();
	boolean _debug = false;
	IRiakClient riakClient;
        Bucket riakBucket;
        String riakBucketName;
    

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DBException {
            String riakTransport = getProperties().getProperty("transport", "pb");
	    String host  = getProperties().getProperty("host", "localhost");
	    String port = getProperties().getProperty("port", "8098");
	    String connectionURL = "http://"+host+":"+port;
	    _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
            riakBucketName = getProperties().getProperty("bucket", "ycsb_bucket");

	    try {
                if (riakTransport.equalsIgnoreCase("http")) {
		    riakClient = RiakFactory.httpClient(connectionURL);
		} else if (riakTransport.equalsIgnoreCase("pb") || riakTransport == null) {
		    riakClient = RiakFactory.pbcClient();
		}
            } catch (RiakException re) {
                re.printStackTrace();
            }

	    if (_debug) {
		System.err.println("Transport protocol: " + riakTransport);
		System.err.println("Connection URL: " + connectionURL);
		System.err.println("Bucket: " + riakBucketName);
	    }

	    try {
		riakClient.ping();
		riakBucket = riakClient.createBucket(riakBucketName).execute();
	    } catch (RiakRetryFailedException rrfe) {
		rrfe.printStackTrace();
	    } catch (RiakException re) {
		re.printStackTrace();
	    }
        }

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	public void cleanup() throws DBException {
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
    public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
        try {
            riakBucket = riakClient.fetchBucket(table).execute();
            IRiakObject fetched = riakBucket.fetch(key).execute();
            String value = fetched.getValueAsString();
        } catch (UnresolvedConflictException uce) {
            uce.printStackTrace();
        } catch (RiakRetryFailedException rrfe) {
            rrfe.printStackTrace();
        } catch (ConversionException ce) {
            ce.printStackTrace();
        }
        return 0;
    }

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {

            return 0;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int update(String table, String key, HashMap<String, String> values) {
		return insert(table, key, values);
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String, String> values) {
		Exception errorexception = null;
                System.out.println("Calling insert with Table " + table + " and key " + key);
		
		try {
		    riakBucket.store(key, "value1").execute();
		} catch (UnresolvedConflictException uce) {
		    uce.printStackTrace();
		} catch (RiakRetryFailedException rrfe) {
		    rrfe.printStackTrace();
		} catch (ConversionException ce) {
		    ce.printStackTrace();
		}
		return 0;
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param bucket The name of the bucket
	 * @param key The key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key) {
            try {
                Bucket b = riakClient.fetchBucket(table).execute();
                b.delete(key).execute();
            } catch( RiakRetryFailedException rfe) {
                rfe.printStackTrace();
            }
            return 0;
	}

	public static void main(String[] args) {
		
            System.err.println("Welcome to the riak client \n");
            RiakClient cli = new RiakClient();
		Properties props = new Properties();

		try {
			cli.init();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		HashMap<String, String> vals = new HashMap<String, String>();
		vals.put("age", "57");
		vals.put("middlename", "bradley");
		vals.put("favoritecolor", "blue");
		int res = cli.insert("usertable", "BrianFrankCooper", vals);
		System.out.println("Result of insert: " + res);

                /*
		HashMap<String, String> result = new HashMap<String, String>();
		HashSet<String> fields = new HashSet<String>();
		fields.add("middlename");
		fields.add("age");
		fields.add("favoritecolor");
		res = cli.read("usertable", "BrianFrankCooper", null, result);
		System.out.println("Result of read: " + res);

		for (String s : result.keySet()) {
			System.out.println("[" + s + "]=[" + result.get(s) + "]");
		}

		res = cli.delete("usertable", "BrianFrankCooper");
		System.out.println("Result of delete: " + res);
	
                */
                }
}
