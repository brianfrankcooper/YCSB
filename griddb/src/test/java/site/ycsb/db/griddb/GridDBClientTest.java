/**
 * Copyright (c) 2018 TOSHIBA Digital Solutions Corporation.
 * Copyright (c) 2018 YCSB contributors.
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
 
package site.ycsb.db.griddb;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeNoException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

public class GridDBClientTest {
    // Default GridbDB port
    private static final int GRIDDB_DEFAULT_PORT = 10040;

    //GridDBDatastore configuration
    private final static String TEST_TABLE = "testtable";
    private final static String NOTIFICATION_PORT = "31999";//default is 31999
    private final static String NOTIFICATION_ADDR = "239.0.0.1";//default is 239.0.0.1
    private final static String CLUSTER_NAME = "ycsbcluster";//Fill your cluster name
    private final static String USER_NAME = "admin";//Fill your user name
    private final static String PASS = "admin";//Fill your password
    private final static int FIELD_COUNT = 10;
    private final static String FIELD_LENGTH = "100";

    private DB myClient = null;
    private final static String DEFAULT_ROW_KEY = "user1";
    public static final String VALUE_COLUMN_NAME_PREFIX= "field";
    private ContainerInfo containerInfo = null;
    private GridStore store;

    /**
    * Verifies the GridDB process (or some process) is running on port 10040, if
    * not the tests are skipped.
    */
    @BeforeClass
    public static void setUpBeforeClass() {
        // Test if we can connect.
        Socket socket = null;
        try {
            // Connect
            socket = new Socket(InetAddress.getLocalHost(), GRIDDB_DEFAULT_PORT);
            assertThat("Socket is not bound.", socket.getLocalPort(), not(-1));
        } catch (IOException connectFailed) {
            assumeNoException("GridDB is not running. Skipping tests.",
            connectFailed);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignore) {
                    // Ignore.
                }
            }
            socket = null;
        }
    }

    /**
     * Create properties for configuration to get client
     * Create data table to test
     */
    @Before
    public void setUp() throws Exception {

        Properties p = new Properties();
        p.setProperty("notificationAddress", NOTIFICATION_ADDR);
        p.setProperty("notificationPort", NOTIFICATION_PORT);
        p.setProperty("clusterName", CLUSTER_NAME);
        p.setProperty("userName", USER_NAME);
        p.setProperty("user", USER_NAME);
        p.setProperty("password", PASS);
        p.setProperty("fieldcount", String.valueOf(FIELD_COUNT));
        p.setProperty("fieldlength", FIELD_LENGTH);

        Measurements.setProperties(p);
        final CoreWorkload workload = new CoreWorkload();
        workload.init(p);
        getDB(p);

        // Create data table to test
        // List of columns
        List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
        ColumnInfo keyInfo = new ColumnInfo("key", GSType.STRING);
        columnInfoList.add(keyInfo);
        for (int i = 0; i < FIELD_COUNT; i++) {
            String columnName = String.format(VALUE_COLUMN_NAME_PREFIX + "%d", i);
            ColumnInfo info = new ColumnInfo(columnName, GSType.STRING);
            columnInfoList.add(info);
        }
        containerInfo = new ContainerInfo(null, ContainerType.COLLECTION, columnInfoList, true);

        try {
            GridStoreFactory.getInstance().setProperties(p);
            store = GridStoreFactory.getInstance().getGridStore(p);
            store.putContainer(TEST_TABLE, containerInfo, false);
        } catch (GSException e) {
            e.printStackTrace();
            throw new DBException();
        }
    }

    /**
     * Insert data to GridbDB database for testing
     */
    private void insertToDatabase() {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        // The number of field in container info is 10
        for (int i = 0; i < FIELD_COUNT; i++) {
            values.put(VALUE_COLUMN_NAME_PREFIX + i, new StringByteIterator("value" + i));
        }
        myClient.insert(TEST_TABLE, DEFAULT_ROW_KEY, values);
    }

    @Test
    public void testReadNoExistedRow() {
        Set<String> fields = Collections.singleton("field0");

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        insertToDatabase();
        Status readStatus = myClient.read(TEST_TABLE, "Missing row", fields, result);

        assertEquals(readStatus, Status.ERROR);
        assertEquals(result.size(), 0);
    }

    @Test
    public void testReadSingleRow() {
        Set<String> fields = Collections.singleton("field1");
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        insertToDatabase();
        Status readStatus = myClient.read(TEST_TABLE, DEFAULT_ROW_KEY, fields, result);

        assertEquals(readStatus, Status.OK);
        assertNotEquals(result.entrySet(), 0);

        for (String key : fields) {
            ByteIterator iter = result.get(key);
            byte[] byteArray1 = iter.toArray();

            String value = new String(byteArray1);
            assertEquals(value, "value1");
        }
    }

    @Test
    public void testReadAll() {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        insertToDatabase();
        Status readStatus = myClient.read(TEST_TABLE, DEFAULT_ROW_KEY, null, result);

        assertEquals(readStatus, Status.OK);
        assertEquals(result.size(), FIELD_COUNT);

        for (int i = 0; i < FIELD_COUNT; i++) {
            ByteIterator iter = result.get("field" + i);
            byte[] byteArray1 = iter.toArray();

            String value = new String(byteArray1);
            assertEquals(value, "value" + i);
        }
    }

    @Test
    public void testUpdate() {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        insertToDatabase();

        String keyForUpdate = "field2";
        Set<String> fields = Collections.singleton(keyForUpdate);

        String strValueToUpdate = "new_value_2";
        ByteIterator valForUpdate = new StringByteIterator(strValueToUpdate);
        values.put(keyForUpdate, valForUpdate);

        Status updateStatus = myClient.update(TEST_TABLE, DEFAULT_ROW_KEY, values);
        assertEquals(updateStatus, Status.OK);

        // After update, we read the update row for get new value
        myClient.read(TEST_TABLE, DEFAULT_ROW_KEY, fields, result);
        assertNotEquals(result.entrySet(), 0);

        boolean found = false;
        for (int i = 0; i < FIELD_COUNT; i++) {
            ByteIterator iter = result.get("field" + i);
            byte[] byteArray1 = iter.toArray();

            String value = new String(byteArray1);
            // check result has row value is new update value or not
            if (value.equals(strValueToUpdate)) {
                found = true;
            }
        }
        assertEquals(found, true);
    }

    @Test
    public void testInsert() {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();

        // The number of field in container info is 10
        for (int i = 0; i < FIELD_COUNT; i++) {
            values.put("field" + i, new StringByteIterator("value" + i));
        }

        Status insertStatus = myClient.insert(TEST_TABLE, DEFAULT_ROW_KEY, values);

        assertEquals(insertStatus, Status.OK);

        myClient.read(TEST_TABLE, DEFAULT_ROW_KEY, null, result);

        assertEquals(result.size(), FIELD_COUNT);

        for (int i = 0; i < FIELD_COUNT; i++) {
            ByteIterator iter = result.get("field" + i);
            byte[] byteArray1 = iter.toArray();

            String value = new String(byteArray1);
            assertEquals(value, "value" + i);
        }
    }

    @Test
    public void testDelete() {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        insertToDatabase();

        Status deleteStatus = myClient.delete(TEST_TABLE, DEFAULT_ROW_KEY);
        assertEquals(deleteStatus, Status.OK);

        Status readStatus = myClient.read(TEST_TABLE, DEFAULT_ROW_KEY, null, result);
        assertEquals(readStatus, Status.ERROR);
        assertEquals(result.size(), 0);
    }

    @Test
    public void testCombination() {
        final int LOOP_COUNT = 3;
        for (int i = 0; i < LOOP_COUNT; i++) {
            testReadNoExistedRow();
            testReadSingleRow();
            testReadAll();
            testInsert();
            testUpdate();
            testDelete();
        }
    }

    /**
     * Stops the test client.
     */
    @After
    public void tearDown() {
        try {
            myClient.cleanup();
            store.dropContainer(TEST_TABLE);
        } catch (Exception error) {
        // Ignore.
        } finally {
            myClient = null;
        }
    }

    /**
     * Gets the test DB.
     *
     * @param props
     *    Properties to pass to the client.
     * @return The test DB.
     */
    protected DB getDB(Properties props) {
        if( myClient == null ) {
            myClient = new GridDBClient();
            myClient.setProperties(props);
            try {
                myClient.init();
            } catch (Exception error) {
                assumeNoException(error);
            }
        }
        return myClient;
    }
}
