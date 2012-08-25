/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author saden
 */
public class ElasticSearchClientTest {

    protected final static ElasticSearchClient instance = new ElasticSearchClient();
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
     * Test of insert method, of class ElasticSearchClient.
     */
    @Test
    public void testInsert() {
        System.out.println("insert");
        int expResult = 0;
        int result = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
        assertEquals(expResult, result);
    }

    /**
     * Test of delete method, of class ElasticSearchClient.
     */
    @Test
    public void testDelete() {
        System.out.println("delete");
        int expResult = 0;
        int result = instance.delete(MOCK_TABLE, MOCK_KEY1);
        assertEquals(expResult, result);
    }

    /**
     * Test of read method, of class ElasticSearchClient.
     */
    @Test
    public void testRead() {
        System.out.println("read");
        Set<String> fields = MOCK_DATA.keySet();
        HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
        int expResult = 0;
        int result = instance.read(MOCK_TABLE, MOCK_KEY1, fields, resultParam);
        assertEquals(expResult, result);
    }

    /**
     * Test of update method, of class ElasticSearchClient.
     */
    @Test
    public void testUpdate() {
        System.out.println("update");
        int i;
        HashMap<String, ByteIterator> newValues = new HashMap<String, ByteIterator>(10);

        for (i = 1; i <= 10; i++) {
            newValues.put("field" + i, new StringByteIterator("newvalue" + i));
        }

        int expResult = 0;
        int result = instance.update(MOCK_TABLE, MOCK_KEY1, newValues);
        assertEquals(expResult, result);

        //validate that the values changed
        HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
        instance.read(MOCK_TABLE, MOCK_KEY1, MOCK_DATA.keySet(), resultParam);

        for (i = 1; i <= 10; i++) {
            assertEquals("newvalue" + i, resultParam.get("field" + i).toString());
        }

    }

    /**
     * Test of scan method, of class ElasticSearchClient.
     */
    @Test
    public void testScan() {
        System.out.println("scan");
        int recordcount = 10;
        Set<String> fields = MOCK_DATA.keySet();
        Vector<HashMap<String, ByteIterator>> resultParam = new Vector<HashMap<String, ByteIterator>>(10);
        int expResult = 0;
        int result = instance.scan(MOCK_TABLE, MOCK_KEY1, recordcount, fields, resultParam);
        assertEquals(expResult, result);
    }
}
