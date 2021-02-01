/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */ 

package site.ycsb.db.elasticsearch7;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.elasticsearch7.ElasticsearchRestHighLevelClient;
import site.ycsb.workloads.CoreWorkload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

@TestMethodOrder(OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
public class ElasticsearchRestHighLevelClientTest {
	
  static Logger logger = Logger.getLogger("ElasticsearchRestHighLevelClientTest"); 
  
  private static ElasticsearchRestHighLevelClient client;
	
  private final static HashMap<String, ByteIterator> MOCK_DATA;
  private final static HashMap<String, ByteIterator> MOCK_TEST;
  private final static String MOCK_TABLE = "MOCK_TABLE";
  private final static String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  static {
    MOCK_DATA = new HashMap<String, ByteIterator>(10);
    for (int i = 1; i <= 10; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
    MOCK_TEST = new HashMap<String, ByteIterator>(1);
    MOCK_TEST.put(FIELD_PREFIX + 8, new StringByteIterator("value" + 9));
    }

  @BeforeAll
  public static void setUp() throws DBException {
    client = new ElasticsearchRestHighLevelClient();
    try {
      client.init();
    } catch (DBException e) {
      logger.info("ERROR " + e);
      e.printStackTrace();
    }
    // Insert 25 documents before doing the test
    for (int i = 0; i < 25; i++) {
      client.insert(MOCK_TABLE, Integer.toString(i), MOCK_DATA);
    }
  }

  @AfterAll
  public void tearDown() throws DBException {
    client.cleanup();
  	logger.info("Disconnected!");
  }
       
  @Test
  @Order(1)  
  public void testInsert() {
    client.getTransportClient();
	final Status result = client.insert(MOCK_TABLE, "12", MOCK_DATA);
	assertEquals(Status.OK, result);
  }
    
  @Test
  @Order(2)  
  public void testDelete() {
    client.getTransportClient();
    final Status result = client.delete(MOCK_TABLE, "5");
    assertEquals(Status.OK, result);
  }
  
  @Test
  @Order(3)  
  public void testRead() {
    client.getTransportClient();
    final Set<String> fields = MOCK_TEST.keySet();
    final HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
    final Status result = client.read(MOCK_TABLE, "1", fields, resultParam);
    assertEquals(Status.OK, result);
  }
  
  @Test
  @Order(4)  
    public void testScan() {
   	  client.getTransportClient();
      final int recordcount = 5;
      final Set<String> fields = MOCK_DATA.keySet();
      final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<HashMap<String, ByteIterator>>(fields.size());
      final Status result = client.scan(MOCK_TABLE, "1", recordcount, fields, resultParam);
      assertEquals(Status.OK, result);
      assertEquals(5, resultParam.size());
  }
    
  @Test
  @Order(5)  
    public void testUpdate() {
      client.getTransportClient();
      final HashMap<String, ByteIterator> newValues = new HashMap<String, ByteIterator>(10);
      for (int i = 1; i <= 10; i++) {
        newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
      }

      final Status updateResult = client.update(MOCK_TABLE, "1", newValues);
      assertEquals(Status.OK, updateResult);

      final HashMap<String, ByteIterator> resultParam = new HashMap<String, ByteIterator>(10);
      final Status readResult = client.read(MOCK_TABLE, "1", MOCK_DATA.keySet(), resultParam);
      assertEquals(Status.OK, readResult);

      for (int i = 1; i <= 10; i++) {
        assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
      }
  }
}