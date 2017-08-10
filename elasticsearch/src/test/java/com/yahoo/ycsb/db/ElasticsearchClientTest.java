/**
 * Copyright (c) 2012-2017 YCSB contributors. All rights reserved.
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;

public class ElasticsearchClientTest {

    @ClassRule public final static TemporaryFolder temp = new TemporaryFolder();
    private final static ElasticsearchClient instance = new ElasticsearchClient();
    private final static HashMap<String, ByteIterator> MOCK_DATA;
    private final static String MOCK_TABLE = "MOCK_TABLE";
    private final static String MOCK_KEY0 = "0";
    private final static String MOCK_KEY1 = "1";
    private final static String MOCK_KEY2 = "2";

    static {
        MOCK_DATA = new HashMap<>(10);
        for (int i = 1; i <= 10; i++) {
            MOCK_DATA.put("field" + i, new StringByteIterator("value" + i));
        }
    }

    @BeforeClass
    public static void setUpClass() throws DBException {
        final Properties props = new Properties();
        props.put("path.home", temp.getRoot().toString());
        instance.setProperties(props);
        instance.init();
    }

    @AfterClass
    public static void tearDownClass() throws DBException {
        instance.cleanup();
    }

    @Before
    public void setUp() {
        instance.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
        instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    }

    @After
    public void tearDown() {
        instance.delete(MOCK_TABLE, MOCK_KEY1);
        instance.delete(MOCK_TABLE, MOCK_KEY2);
    }

    /**
     * Test of insert method, of class ElasticsearchClient.
     */
    @Test
    public void testInsert() {
        Status result = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of delete method, of class ElasticsearchClient.
     */
    @Test
    public void testDelete() {
        Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of read method, of class ElasticsearchClient.
     */
    @Test
    public void testRead() {
        Set<String> fields = MOCK_DATA.keySet();
        HashMap<String, ByteIterator> resultParam = new HashMap<>(10);
        Status result = instance.read(MOCK_TABLE, MOCK_KEY1, fields, resultParam);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of update method, of class ElasticsearchClient.
     */
    @Test
    public void testUpdate() {
        int i;
        HashMap<String, ByteIterator> newValues = new HashMap<>(10);

        for (i = 1; i <= 10; i++) {
            newValues.put("field" + i, new StringByteIterator("newvalue" + i));
        }

        Status result = instance.update(MOCK_TABLE, MOCK_KEY1, newValues);
        assertEquals(Status.OK, result);

        //validate that the values changed
        HashMap<String, ByteIterator> resultParam = new HashMap<>(10);
        instance.read(MOCK_TABLE, MOCK_KEY1, MOCK_DATA.keySet(), resultParam);

        for (i = 1; i <= 10; i++) {
            assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
        }
    }

    /**
     * Test of scan method, of class ElasticsearchClient.
     */
    @Test
    public void testScan() {
        int recordcount = 10;
        Set<String> fields = MOCK_DATA.keySet();
        Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(10);
        Status result = instance.scan(MOCK_TABLE, MOCK_KEY1, recordcount, fields, resultParam);
        assertEquals(Status.OK, result);
    }
}
