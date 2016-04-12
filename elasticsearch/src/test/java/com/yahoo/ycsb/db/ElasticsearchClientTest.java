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

import static org.testng.AssertJUnit.assertEquals;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class ElasticsearchClientTest {

    protected final static ElasticsearchClient instance = new ElasticsearchClient();
    protected final static HashMap<String, ByteIterator> MOCK_DATA;
    protected final static String MOCK_TABLE = "MOCK_TABLE";
    protected final static String MOCK_KEY0 = "0";
    protected final static String MOCK_KEY1 = "1";
    protected final static String MOCK_KEY2 = "2";

    static {
        MOCK_DATA = new HashMap<String, ByteIterator>(10);
        for (int i = 1; i <= 10; i++) {
            MOCK_DATA.put("field" + i, new StringByteIterator("value" + i));
        }
    }

    @BeforeClass
    public static void setUpClass() throws DBException {
        instance.init();
    }

    @AfterClass
    public static void tearDownClass() throws DBException {
        instance.cleanup();
    }

    @BeforeMethod
    public void setUp() {
        instance.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
        instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    }

    @AfterMethod
    public void tearDown() {
        instance.delete(MOCK_TABLE, MOCK_KEY1);
        instance.delete(MOCK_TABLE, MOCK_KEY2);
    }

    /**
     * Test of insert method, of class ElasticsearchClient.
     */
    @Test
    public void testInsert() {
        System.out.println("insert");
        Status result = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of delete method, of class ElasticsearchClient.
     */
    @Test
    public void testDelete() {
        System.out.println("delete");
        Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of read method, of class ElasticsearchClient.
     */
    @Test
    public void testRead() {
        System.out.println("read");
        Set<String> fields = MOCK_DATA.keySet();
        HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
        Status result = instance.read(MOCK_TABLE, MOCK_KEY1, fields, resultParam);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of update method, of class ElasticsearchClient.
     */
    @Test
    public void testUpdate() {
        System.out.println("update");
        int i;
        HashMap<String, ByteIterator> newValues = new HashMap<String, ByteIterator>(10);

        for (i = 1; i <= 10; i++) {
            newValues.put("field" + i, new StringByteIterator("newvalue" + i));
        }

        Status result = instance.update(MOCK_TABLE, MOCK_KEY1, newValues);
        assertEquals(Status.OK, result);

        //validate that the values changed
        HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
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
        System.out.println("scan");
        int recordcount = 10;
        Set<String> fields = MOCK_DATA.keySet();
        Vector<HashMap<String, ByteIterator>> resultParam = new Vector<HashMap<String, ByteIterator>>(10);
        Status result = instance.scan(MOCK_TABLE, MOCK_KEY1, recordcount, fields, resultParam);
        assertEquals(Status.OK, result);
    }
}
