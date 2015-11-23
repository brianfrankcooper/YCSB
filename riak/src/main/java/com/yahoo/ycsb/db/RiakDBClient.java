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

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

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

	private boolean _debug = false;

	public static final String HOST_PROPERTY = "riak.hosts";
    public static final String PORT_PROPERTY = "riak.port";
    public static final String BUCKET_TYPE_PROPERTY = "riak.bucket_type";
    public static final String R_VALUE_PROPERTY = "riak.r_val";
    public static final String W_VALUE_PROPERTY = "riak.w_val";
    public static final String READ_RETRY_COUNT_PROPERTY = "riak.read_retry_count";

    public static final String[] DEFAULT_HOSTS = {"127.0.0.1"};
    public static final int DEFAULT_PORT = 8087;
    public static final String DEFAULT_BUCKET_TYPE = "ycsb";
    public static int DEFAULT_R_VALUE = 2;
    public static int DEFAULT_W_VALUE = 2;
	public static int DEFAULT_READ_RETRY_COUNT_VALUE = 5;

	private String[] hosts = DEFAULT_HOSTS;
	private int port = DEFAULT_PORT;
	private String bucketType = DEFAULT_BUCKET_TYPE;
	private int rvalue = DEFAULT_R_VALUE;
	private int wvalue = DEFAULT_W_VALUE;
	private int readRetryCount = DEFAULT_READ_RETRY_COUNT_VALUE;
	
	private RiakClient riakClient;
	private RiakCluster riakCluster;

	public void init() throws DBException {
		loadProperties();

		if (_debug) {
			System.out.println("Hosts: " + Arrays.toString(hosts));
			System.out.println("Port: " + port);
			System.out.println("Bucket Type: " + bucketType);
			System.out.println("R Val: " + rvalue);
			System.out.println("W Val: " + wvalue);
			System.out.println("Read Retry Count: " + readRetryCount);
		}

		final RiakNode.Builder builder = new RiakNode.Builder();
        List<RiakNode> nodes;
		try {
			nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
			riakCluster = new RiakCluster.Builder(nodes).build();
	        riakCluster.start();
	        riakClient = new RiakClient(riakCluster);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
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
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
        	final Location location = new Location(new Namespace(bucketType, table), key);
            final FetchValue fv = new FetchValue.Builder(location)
            	.withOption(FetchValue.Option.R, new Quorum(rvalue))
            	.build();
            final FetchValue.Response response = fetch(fv);
            if (response.isNotFound()) {
            	return Status.ERROR;
            }
            return Status.OK;
        } 
        catch (Exception e) {
            e.printStackTrace();
            return Status.ERROR;
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
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
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
	            	return Status.ERROR;
	            }
			}
			return Status.OK;
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Status.ERROR;
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
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		System.out.println(key + ": " + BinaryValue.create(serializeTable(values)));
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
            return Status.OK;
        } 
        catch (Exception e) {
            e.printStackTrace();
            return Status.ERROR;
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
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
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
	public Status delete(String table, String key) {
        try {
        	final Location location = new Location(new Namespace(bucketType, table), key);
            final DeleteValue dv = new DeleteValue.Builder(location).build();
            riakClient.execute(dv);
        } catch (Exception e) {
            e.printStackTrace();
            return Status.ERROR;
        }
        return Status.OK;
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

	private void loadProperties()
	{
		Properties props = getProperties();

		String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null) {
            port = Integer.parseInt(portString);
        }

        String hostsString = props.getProperty(HOST_PROPERTY);
        if (hostsString != null) {
            hosts = hostsString.split(",");
        }

        String bucketTypeString = props.getProperty(BUCKET_TYPE_PROPERTY);
        if (bucketTypeString != null) {
        	bucketType = bucketTypeString;
        }

        String rvalueString = props.getProperty(R_VALUE_PROPERTY);
        if (rvalueString != null) {
        	rvalue = Integer.parseInt(rvalueString);
        }

        String wvalueString = props.getProperty(W_VALUE_PROPERTY);
        if (wvalueString != null) {
        	wvalue = Integer.parseInt(wvalueString);
        }

        String readRetryCountString = props.getProperty(READ_RETRY_COUNT_PROPERTY);
        if (readRetryCountString != null) {
        	readRetryCount = Integer.parseInt(readRetryCountString);
        }

        String debugString = props.getProperty("debug");
        if (debugString != null)
        {
        	_debug = Boolean.parseBoolean(debugString);
        }
	}
}
