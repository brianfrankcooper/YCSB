/*
 * Copyright 2015 Basho Technologies, Inc.
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

import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.yahoo.ycsb.ByteIterator;

import static com.yahoo.ycsb.db.RiakUtils.*;

/**
 * @author Brian McClain <bmcclain at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public final class RiakKVDBClient extends AbstractRiakClient {
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
        	final Location location = config().mkLocationFor(table, key);
            final FetchValue fv = new FetchValue.Builder(location)
            	.withOption(FetchValue.Option.R, config().readQuorum())
            	.build();

            final FetchValue.Response response = fetchValue(fv);

            if (response.isNotFound()) {
                logger.error( "READ FAILED: NOT FOUND");
            	return Status.ERROR;
            }
            return Status.OK;
        } 
        catch (Exception e) {
            logger.error( "READ FAILED: UE", e);
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
			final Namespace ns = config().mkNamespaceFor(table);

			final IntIndexQuery iiq = new IntIndexQuery
				.Builder(ns, "key", getKeyAsLong(startkey), 999999999999999999L)
				.withMaxResults(recordcount)
				.withPaginationSort(true)
	        	.build();

			final IntIndexQuery.Response response = riakClient.execute(iiq);
			final List<IntIndexQuery.Response.Entry> entries = response.getEntries();
			
			for (int i = 0; i < entries.size(); ++i ) {
				final Location location = entries.get(i).getRiakObjectLocation();

                final FetchValue fv = new FetchValue.Builder(location)
	            	.withOption(FetchValue.Option.R, config().readQuorum())
	            	.build();
				
	            final FetchValue.Response keyResponse = fetchValue(fv);
	            if (keyResponse.isNotFound()) {
                    logger.error( "SCAN FAILED: NOT FOUND for {}", location);
	            	return Status.ERROR;
	            }
			}
			return Status.OK;
		} catch (Exception e) {
			logger.error("SCAN FAILED: UE", e);
		}
		return Status.ERROR;
	}
	
	
	private FetchValue.Response fetchValue(FetchValue fv) {
		try {
			FetchValue.Response response = null;
			for (int i = 0; i < config().getReadRetry(); ++i) {
				response = riakClient.execute(fv);
				if (!response.isNotFound()){
                    break;
                }
			}
			return response;
        } catch (Exception e) {
            if( e instanceof RuntimeException) {
                throw (RuntimeException)e;
            }
            throw new RuntimeException(e);
        }
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
        try {
            final Location location = config().mkLocationFor(table, key);

            final RiakObject object = new RiakObject();
            object.setValue(BinaryValue.create(serializeTable(values)));
            object.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));
            StoreValue store = new StoreValue.Builder(object)
            	.withLocation(location)
                .withOption(Option.W, config().writeQuorum())
                .build();
            riakClient.execute(store);
            return Status.OK;
        } 
        catch (Exception e) {
            logger.error("INSERT FAILED: UE", e);
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
            final Location location = config().mkLocationFor(table, key);

            final DeleteValue dv = new DeleteValue.Builder(location).build();
            riakClient.execute(dv);
        } catch (Exception e) {
            logger.error("DELETE FAILED: UE", e);
            return Status.ERROR;
        }
        return Status.OK;
	}
}
