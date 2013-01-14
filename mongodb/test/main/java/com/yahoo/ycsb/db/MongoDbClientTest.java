package com.yahoo.ycsb.db;

import com.mongodb.WriteConcern;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MongoDbClientTest {

    MongoDbClient client;
    Properties props;

    @Before
    public void setUp() {
        client = new MongoDbClient();
        props = new Properties();
    }

    @Test
    public void testInitWriteConcernDefault() throws Exception {
        client.init();
        assertEquals(WriteConcern.SAFE, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernNormal() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        client.setProperties(props);
        client.init();
        assertEquals(WriteConcern.NORMAL, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernW() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        props.setProperty("mongodb.writeConcern.w", "2");
        client.setProperties(props);
        client.init();
        WriteConcern concern = new WriteConcern(2, 0, false, false, false);
        assertEquals(concern, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernWtimeout() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        props.setProperty("mongodb.writeConcern.wtimeout", "500");
        client.setProperties(props);
        client.init();
        WriteConcern concern = new WriteConcern(0, 500, false, false, false);
        assertEquals(concern, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernFsync() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        props.setProperty("mongodb.writeConcern.fsync", "true");
        client.setProperties(props);
        client.init();
        WriteConcern concern = new WriteConcern(0, 0, true, false, false);
        assertEquals(concern, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernJ() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        props.setProperty("mongodb.writeConcern.j", "true");
        client.setProperties(props);
        client.init();
        WriteConcern concern = new WriteConcern(0, 0, false, true, false);
        assertEquals(concern, client.writeConcern);
    }

    @Test
    public void testInitWriteConcernContinue() throws Exception {
        props.setProperty("mongodb.writeConcern", "normal");
        props.setProperty("mongodb.writeConcern.continueOnErrorForInsert", "true");
        client.setProperties(props);
        client.init();
        WriteConcern concern = new WriteConcern(0, 0, false, false, true);
        assertEquals(concern, client.writeConcern);
    }

}
