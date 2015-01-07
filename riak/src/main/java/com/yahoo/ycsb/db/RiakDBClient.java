/*
 * Copyright 2014 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.ycsb.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import static com.yahoo.ycsb.db.RiakUtils.*;

/**
 * @author Basho Technologies, Inc.
 */
public final class RiakDBClient extends DB {

	// Array of nodes in the Riak cluster or load balancer in front of the cluster, 
	// IP Addresses or Fully Qualified Domain Names (FQDNs)
	// e.g.: {"127.0.0.1","127.0.0.2","127.0.0.3","127.0.0.4","127.0.0.5"} or
	// {"riak1.mydomain.com","riak2.mydomain.com","riak3.mydomain.com","riak4.mydomain.com","riak5.mydomain.com"}
	private String[] nodesArray = {"127.0.0.1"};
	
	// Note: DEFAULT_BUCKET_TYPE value is set when configuring
	// the Riak cluster as described in the project README.md
	private String bucketType = "ycsb";
	
	private int rvalue = 2;
	private int wvalue = 2;
	private int readRetryCount = 5;
	
	private RiakClient riakClient;
	private RiakCluster riakCluster;
	
	
	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table (Riak bucket)
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
        	final Location location = new Location(new Namespace(bucketType, table), key);
            final FetchValue fv = new FetchValue.Builder(location)
            	.withOption(FetchValue.Option.R, new Quorum(rvalue))
            	.build();
            final FetchValue.Response response = fetch(fv);
            if (response.isNotFound()) {
            	return 1;
            }
            return 0;
        } 
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
	}
	
	
	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * Note: The scan operation requires the use of secondary indexes (2i) and LevelDB. 
	 *
	 * @param table The name of the table (Riak bucket)
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		try {
			Namespace ns = new Namespace(bucketType, table);
			IntIndexQuery iiq = new IntIndexQuery
				.Builder(ns, "key", getKeyAsLong(startkey), 999999999999999999L)
				.withMaxResults(recordcount)
				.withPaginationSort(true)
	        	.build();
			IntIndexQuery.Response response = riakClient.execute(iiq);
			List<IntIndexQuery.Response.Entry> entries = response.getEntries();
			
			for (int i = 0; i < entries.size(); i++ ) {
				final Location location = entries.get(i).getRiakObjectLocation();
				final FetchValue fv = new FetchValue.Builder(location)
	            	.withOption(FetchValue.Option.R, new Quorum(rvalue))
	            	.build();
				
	            final FetchValue.Response keyResponse = fetch(fv);
	            if (keyResponse.isNotFound()) {
	            	return 1;
	            }
			}
			return 0;
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 1;
	}
	
	
	private FetchValue.Response fetch(FetchValue fv) {
		try {
			FetchValue.Response response = null;
			for (int i = 0; i < readRetryCount; i++) {
				response = riakClient.execute(fv);
				if (response.isNotFound() == false) break;
			}
			return response;
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}


	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap 
	 * will be written into the record with the specified record key. Also creates a
	 * secondary index (2i) for each record consisting of the key converted to long to be used
	 * for the scan operation
	 *
	 * @param table The name of the table (Riak bucket)
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
        	final Location location = new Location(new Namespace(bucketType, table), key);
            final RiakObject object = new RiakObject();
            object.setValue(BinaryValue.create(serializeTable(values)));
            object.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));
            StoreValue store = new StoreValue.Builder(object)
            	.withLocation(location)
                .withOption(Option.W, new Quorum(wvalue))
                .build();
            riakClient.execute(store);
            return 0;
        } 
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
	}
	
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values 
	 * HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table (Riak bucket)
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return insert(table, key, values);
	}
		

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table (Riak bucket)
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int delete(String table, String key) {
        try {
        	final Location location = new Location(new Namespace(bucketType, table), key);
            final DeleteValue dv = new DeleteValue.Builder(location).build();
            riakClient.execute(dv);
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
	}

	
	

	public void init() throws DBException {
		loadProperties();
		final RiakNode.Builder builder = new RiakNode.Builder();
        List<RiakNode> nodes;
		try {
			nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(nodesArray));
			riakCluster = new RiakCluster.Builder(nodes).build();
	        riakCluster.start();
	        riakClient = new RiakClient(riakCluster);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	private boolean loadProperties() {
		Properties defaultProps = new Properties();
		try
		{
			File f = getPropertiesFile();
			if(f.exists() && !f.isDirectory()) {
				FileInputStream propertyFile = new FileInputStream(f);
				defaultProps.load(propertyFile);
				propertyFile.close();
				
				nodesArray = defaultProps.getProperty("NODES", "127.0.0.1").split(",");
				bucketType = defaultProps.getProperty("DEFAULT_BUCKET_TYPE","ycsb");
				rvalue = Integer.parseInt( defaultProps.getProperty("R_VALUE", "2") );
				wvalue = Integer.parseInt( defaultProps.getProperty("W_VALUE", "2") );
				readRetryCount = Integer.parseInt( defaultProps.getProperty("READ_RETRY_COUNT", "5") );			
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private File getPropertiesFile() {
		File f = new File("riak-binding/lib/riak.properties");
		if(f.exists() && !f.isDirectory()) {
			return f;
		}
		else {
			f = new File("riak/target/riak.properties");
			if(f.exists() && !f.isDirectory()) {
				return f;
			}
			else {
				URL url = RiakDBClient.class.getProtectionDomain().getCodeSource().getLocation();
				String jarPath = null;
				try {
					jarPath = URLDecoder.decode(url.getFile(), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					return null;
				}
				f = new File(jarPath + "riak.properties");
				return f;
			}
		}
	}

	public void cleanup() throws DBException
	{
		try {
			riakCluster.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
