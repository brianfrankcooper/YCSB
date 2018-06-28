/*
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.db.elasticsearch5;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.junit.Assert.assertEquals;

public abstract class ElasticsearchIntegTestBase {

    private DB db;

    abstract DB newDB();

    private final static HashMap<String, ByteIterator> MOCK_DATA;
    private final static String MOCK_TABLE = "MOCK_TABLE";

    static {
        MOCK_DATA = new HashMap<>(10);
        for (int i = 1; i <= 10; i++) {
            MOCK_DATA.put("field" + i, new StringByteIterator("value" + i));
        }
    }

    @Before
    public void setUp() throws DBException {
        final Properties props = new Properties();
        props.put("es.new_index", "true");
        props.put("es.setting.cluster.name", "test");
        db = newDB();
        db.setProperties(props);
        db.init();
        for (int i = 0; i < 16; i++) {
            db.insert(MOCK_TABLE, Integer.toString(i), MOCK_DATA);
        }
    }

    @After
    public void tearDown() throws DBException {
        db.cleanup();
    }

    @Test
    public void testInsert() {
        final Status result = db.insert(MOCK_TABLE, "0", MOCK_DATA);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of delete method, of class ElasticsearchClient.
     */
    @Test
    public void testDelete() {
        final Status result = db.delete(MOCK_TABLE, "1");
        assertEquals(Status.OK, result);
    }

    /**
     * Test of read method, of class ElasticsearchClient.
     */
    @Test
    public void testRead() {
        final Set<String> fields = MOCK_DATA.keySet();
        final HashMap<String, ByteIterator> resultParam = new HashMap<>(10);
        final Status result = db.read(MOCK_TABLE, "1", fields, resultParam);
        assertEquals(Status.OK, result);
    }

    /**
     * Test of update method, of class ElasticsearchClient.
     */
    @Test
    public void testUpdate() {
        final HashMap<String, ByteIterator> newValues = new HashMap<>(10);

        for (int i = 1; i <= 10; i++) {
            newValues.put("field" + i, new StringByteIterator("newvalue" + i));
        }

        final Status updateResult = db.update(MOCK_TABLE, "1", newValues);
        assertEquals(Status.OK, updateResult);

        // validate that the values changed
        final HashMap<String, ByteIterator> resultParam = new HashMap<>(10);
        final Status readResult = db.read(MOCK_TABLE, "1", MOCK_DATA.keySet(), resultParam);
        assertEquals(Status.OK, readResult);

        for (int i = 1; i <= 10; i++) {
            assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
        }

    }

    /**
     * Test of scan method, of class ElasticsearchClient.
     */
    @Test
    public void testScan() {
        final int recordcount = 10;
        final Set<String> fields = MOCK_DATA.keySet();
        final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(10);
        final Status result = db.scan(MOCK_TABLE, "1", recordcount, fields, resultParam);
        assertEquals(Status.OK, result);

        assertEquals(10, resultParam.size());
    }

}
