/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;


public class Neo4jClientTest {
	protected final static Neo4jClient instance = new Neo4jClient();
	protected final static HashMap<String, ByteIterator> NEO_DATA;
	protected final static String TABLE_LABEL = "TESTLABEL";
	protected final static String KEY_0 = "0";
	protected final static String KEY_1 = "1";
	protected final static String KEY_2 = "2";
	protected static File basePath;

	static {
		NEO_DATA = new HashMap<String, ByteIterator>(0);
		for (int i = 1; i <= 10; i++) {
			NEO_DATA.put("field" + i, new StringByteIterator("value" + i));
		}
	}

	/**
	 * creating a test database
	 * @throws DBException
	 */
	@BeforeClass
	public static void setUpClass() throws DBException {
		// creating test db
		basePath = new File("test.db");

		Properties p = new Properties();
		p.setProperty(Neo4jClient.BASE_PATH,basePath.getAbsolutePath());

		instance.setProperties(p);

		instance.init();
		// used in read, scan & update tests
		instance.insert(TABLE_LABEL, KEY_0, NEO_DATA);
		// used in delete test
		instance.insert(TABLE_LABEL, KEY_2, NEO_DATA);
	}

	@AfterClass
	public static void tearDownClass() throws DBException {
		instance.cleanup();
		deleteFileOrDirectory(basePath);
	}

	/*
	used to delete graph database folder
	 */
	public static void deleteFileOrDirectory( final File file ) {
		if ( file.exists() ) {
			if ( file.isDirectory() ) {
				for ( File child : file.listFiles() ) {
					deleteFileOrDirectory( child );
				}
			}
			file.delete();
		}
	}

	@Test
	public void testInsert() {
		Status result = instance.insert(TABLE_LABEL, KEY_1, NEO_DATA);
		assertEquals(Status.OK, result);
		// verify that result is coherent
		Set<String> fields = NEO_DATA.keySet();
		HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
		result = instance.read(TABLE_LABEL, KEY_0, fields, resultParam);

		//validate that the values are correct
		for (int i = 1; i <= 10; i++) {
			assertEquals("value" + i, resultParam.get("field" + i).toString());
		}
	}

	@Test
	public void testDelete() {
		Status result = instance.delete(TABLE_LABEL, KEY_2);
		assertEquals(Status.OK, result);
		// verify that result is coherent
		Set<String> fields = NEO_DATA.keySet();
		HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
		result = instance.read(TABLE_LABEL, KEY_0, fields, resultParam);

		//validate that the values are correct
		for (int i = 1; i <= 10; i++) {
			assertEquals("value" + i, resultParam.get("field" + i).toString());
		}
	}

	@Test
	public void testRead() {
		Set<String> fields = NEO_DATA.keySet();
		HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
		Status result = instance.read(TABLE_LABEL, KEY_0, fields, resultParam);
		assertEquals(Status.OK, result);

		//validate that the values are correct
		for (int i = 1; i <= 10; i++) {
			assertEquals("value" + i, resultParam.get("field" + i).toString());
		}
	}


	@Test
	public void testUpdate() {
		int i;
		HashMap<String, ByteIterator> newValues = new HashMap<String, ByteIterator>(10);

		for (i = 1; i <= 10; i++) {
			newValues.put("field" + i, new StringByteIterator("newvalue" + i));
		}

		Status result = instance.update(TABLE_LABEL, KEY_0, newValues);
		assertEquals(Status.OK, result);

		//validate that the values have changed
		HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
		instance.read(TABLE_LABEL, KEY_0, NEO_DATA.keySet(), resultParam);

		for (i = 1; i <= 10; i++) {
			assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
		}
	}

	@Test
	public void testScan() {
		int recordCount = 2;
		Set<String> fields = NEO_DATA.keySet();
		Vector<HashMap<String, ByteIterator>> resultParam = new Vector<HashMap<String, ByteIterator>>(10);
		Status result = instance.scan(TABLE_LABEL, KEY_0, recordCount, fields, resultParam);
		assertEquals(Status.OK, result);

		// Checking that the resultVector is the correct size
		assertEquals(recordCount, resultParam.size());
		// Checking each vector row to make sure we have the correct fields
		for (HashMap<String, ByteIterator> rP: resultParam) {
			// size check
			assertEquals(fields.size(), rP.size());
			for (String field: fields) {
				// data correspondance check
				assertEquals(NEO_DATA.get(field).toString(), rP.get(field).toString());
			}
		}
	}

}
