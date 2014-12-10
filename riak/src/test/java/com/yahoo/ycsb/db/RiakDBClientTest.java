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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.RiakDBClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Basho Technologies, Inc.
 */
public class RiakDBClientTest {
	RiakDBClient cli;
	String bucket = "people";
    String key = "person1";
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		cli = new RiakDBClient();
		cli.init();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		cli.cleanup();
	}

	/**
	 * Test method for RiakDBClient.read(java.lang.String, java.lang.String, java.util.Set, java.util.HashMap)
	 */
	@Test
	public void testRead() {
		Set<String> fields = new HashSet<String>();
        fields.add("first_name");
        fields.add("last_name");
        fields.add("city");
        HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
        // .read returns 0 on success, 1 on failure
        assertEquals(0, cli.read(bucket, key, fields, results));
	}

	/**
	 * Test method for RiakDBClient.scan(java.lang.String, java.lang.String, int, java.util.Set, java.util.Vector)
	 */
	@Test
	public void testScan() {
		Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
		// .scan returns 0 on success, 1 on failure
		assertEquals(0, cli.scan("usertable", "user5947069136552588163", 7, null, results));
	}

	/**
	 * Test method for RiakDBClient.update(java.lang.String, java.lang.String, java.util.HashMap)
	 */
	@Test
	public void testUpdate() {
		HashMap<String, String> values = new HashMap<String, String>();
        values.put("first_name", "Dave");
        values.put("last_name", "Parfitt");
        values.put("city", "Buffalo, NY");
        // .update returns 0 on success, 1 on failure
		assertEquals(0, cli.update(bucket, key, StringByteIterator.getByteIteratorMap(values)));
	}

	/**
	 * Test method for RiakDBClient.insert(java.lang.String, java.lang.String, java.util.HashMap)
	 */
	@Test
	public void testInsert() {
		HashMap<String, String> values = new HashMap<String, String>();
        values.put("first_name", "Dave");
        values.put("last_name", "Parfitt");
        values.put("city", "Buffalo, NY");
        // .insert returns 0 on success, 1 on failure
		assertEquals(0, cli.insert(bucket, key, StringByteIterator.getByteIteratorMap(values)));
	}

	/**
	 * Test method for RiakDBClient.delete(java.lang.String, java.lang.String)
	 */
	@Test
	public void testDelete() {
        // .delete returns 0 on success, 1 on failure
        assertEquals(0, cli.delete(bucket, key));
	}

}
