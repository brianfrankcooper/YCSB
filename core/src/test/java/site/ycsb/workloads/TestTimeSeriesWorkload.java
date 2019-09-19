/**
 * Copyright (c) 2017 YCSB contributors All rights reserved.
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
package site.ycsb.workloads;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.NumericByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Utils;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

import org.testng.annotations.Test;

public class TestTimeSeriesWorkload {
  
  @Test
  public void twoThreads() throws Exception {
    final Properties p = getUTProperties();
    Measurements.setProperties(p);
    
    final TimeSeriesWorkload wl = new TimeSeriesWorkload();
    wl.init(p);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAB");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
  }
  
  @Test (expectedExceptions = WorkloadException.class)
  public void badTimeUnit() throws Exception {
    final Properties p = new Properties();
    p.put(TimeSeriesWorkload.TIMESTAMP_UNITS_PROPERTY, "foobar");
    getWorkload(p, true);
  }
  
  @Test (expectedExceptions = WorkloadException.class)
  public void failedToInitWorkloadBeforeThreadInit() throws Exception {
    final Properties p = getUTProperties();
    final TimeSeriesWorkload wl = getWorkload(p, false);
    //wl.init(p); // <-- we NEED this :(
    final Object threadState = wl.initThread(p, 0, 2);
    
    final MockDB db = new MockDB();
    wl.doInsert(db, threadState);
  }
  
  @Test (expectedExceptions = IllegalStateException.class)
  public void failedToInitThread() throws Exception {
    final Properties p = getUTProperties();
    final TimeSeriesWorkload wl = getWorkload(p, true);
    
    final MockDB db = new MockDB();
    wl.doInsert(db, null);
  }

  @Test
  public void insertOneKeyOneTagCardinalityOne() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    p.put(TimeSeriesWorkload.TAG_COUNT_PROPERTY, "1");
    p.put(TimeSeriesWorkload.TAG_CARDINALITY_PROPERTY, "1");
    final TimeSeriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);

    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      timestamp += 60;
    }
  }
  
  @Test
  public void insertOneKeyTwoTagsLowCardinality() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    final TimeSeriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void insertTwoKeysTwoTagsLowCardinality() throws Exception {
    final Properties p = getUTProperties();
    
    final TimeSeriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    int metricCtr = 0;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      }
      if (metricCtr++ > 1) {
        assertEquals(db.keys.get(i), "AAAB");
        if (metricCtr >= 4) {
          metricCtr = 0;
          timestamp += 60;
        }
      } else {
        assertEquals(db.keys.get(i), "AAAA");
      }
    }
  }
  
  @Test
  public void insertTwoKeysTwoThreads() throws Exception {
    final Properties p = getUTProperties();
    
    final TimeSeriesWorkload wl = getWorkload(p, true);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA"); // <-- key 1
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAB"); // <-- key 2
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void insertThreeKeysTwoThreads() throws Exception {
    // To make sure the distribution doesn't miss any metrics
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "3");
    
    final TimeSeriesWorkload wl = getWorkload(p, true);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    timestamp = 1451606400;
    int metricCtr = 0;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      }
      if (metricCtr++ > 1) {
        assertEquals(db.keys.get(i), "AAAC");
        if (metricCtr >= 4) {
          metricCtr = 0;
          timestamp += 60;
        }
      } else {
        assertEquals(db.keys.get(i), "AAAB");
      }
    }
  }
  
  @Test
  public void insertWithValidation() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    p.put(CoreWorkload.DATA_INTEGRITY_PROPERTY, "true");
    p.put(TimeSeriesWorkload.VALUE_TYPE_PROPERTY, "integers");
    final TimeSeriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT).toArray()), timestamp);
      assertFalse(((NumericByteIterator) db.values.get(i)
          .get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).isFloatingPoint());
      
      // validation check
      final TreeMap<String, String> validationTags = new TreeMap<String, String>();
      for (final Entry<String, ByteIterator> entry : db.values.get(i).entrySet()) {
        if (entry.getKey().equals(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT) || 
            entry.getKey().equals(TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT)) {
          continue;
        }
        validationTags.put(entry.getKey(), entry.getValue().toString());
      }
      assertEquals(wl.validationFunction(db.keys.get(i), timestamp, validationTags), 
          ((NumericByteIterator) db.values.get(i).get(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT)).getLong());
      
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAA");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void read() throws Exception {
    final Properties p = getUTProperties();
    final TimeSeriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 20; i++) {
      wl.doTransactionRead(db, threadState);
    }
  }
  
  @Test
  public void verifyRow() throws Exception {
    final Properties p = getUTProperties();
    final TimeSeriesWorkload wl = getWorkload(p, true);
    
    final TreeMap<String, String> validationTags = new TreeMap<String, String>();
    final HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    
    validationTags.put("AA", "AAAA");
    cells.put("AA", new StringByteIterator("AAAA"));
    validationTags.put("AB", "AAAB");
    cells.put("AB", new StringByteIterator("AAAB"));
    long hash = wl.validationFunction("AAAA", 1451606400L, validationTags);
        
    cells.put(TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT, new NumericByteIterator(1451606400L));
    cells.put(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT, new NumericByteIterator(hash));
    
    assertEquals(wl.verifyRow("AAAA", cells), Status.OK);
    
    // tweak the last value a bit
    for (final ByteIterator it : cells.values()) {
      it.reset();
    }
    cells.put(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT, new NumericByteIterator(hash + 1));
    assertEquals(wl.verifyRow("AAAA", cells), Status.UNEXPECTED_STATE);
    
    // no value cell, returns an unexpected state
    for (final ByteIterator it : cells.values()) {
      it.reset();
    }
    cells.remove(TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT);
    assertEquals(wl.verifyRow("AAAA", cells), Status.UNEXPECTED_STATE);
  }
  
  @Test
  public void validateSettingsDataIntegrity() throws Exception {
    Properties p = getUTProperties();
    
    // data validation incompatibilities
    p.setProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY, "true");
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
    
    p.setProperty(TimeSeriesWorkload.VALUE_TYPE_PROPERTY, "integers"); // now it's ok
    p.setProperty(TimeSeriesWorkload.GROUPBY_PROPERTY, "sum"); // now it's not
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
    
    p.setProperty(TimeSeriesWorkload.GROUPBY_PROPERTY, "");
    p.setProperty(TimeSeriesWorkload.DOWNSAMPLING_FUNCTION_PROPERTY, "sum");
    p.setProperty(TimeSeriesWorkload.DOWNSAMPLING_INTERVAL_PROPERTY, "60");
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
    
    p.setProperty(TimeSeriesWorkload.DOWNSAMPLING_FUNCTION_PROPERTY, "");
    p.setProperty(TimeSeriesWorkload.DOWNSAMPLING_INTERVAL_PROPERTY, "");
    p.setProperty(TimeSeriesWorkload.QUERY_TIMESPAN_PROPERTY, "60");
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
    
    p = getUTProperties();
    p.setProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY, "true");
    p.setProperty(TimeSeriesWorkload.VALUE_TYPE_PROPERTY, "integers");
    p.setProperty(TimeSeriesWorkload.RANDOMIZE_TIMESERIES_ORDER_PROPERTY, "true");
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
    
    p.setProperty(TimeSeriesWorkload.RANDOMIZE_TIMESERIES_ORDER_PROPERTY, "false");
    p.setProperty(TimeSeriesWorkload.INSERT_START_PROPERTY, "");
    try {
      getWorkload(p, true);
      fail("Expected WorkloadException");
    } catch (WorkloadException e) { }
  }
  
  /** Helper method that generates unit testing defaults for the properties map */
  private Properties getUTProperties() {
    final Properties p = new Properties();
    p.put(Client.RECORD_COUNT_PROPERTY, "10");
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "2");
    p.put(CoreWorkload.FIELD_LENGTH_PROPERTY, "4");
    p.put(TimeSeriesWorkload.TAG_KEY_LENGTH_PROPERTY, "2");
    p.put(TimeSeriesWorkload.TAG_VALUE_LENGTH_PROPERTY, "4");
    p.put(TimeSeriesWorkload.TAG_COUNT_PROPERTY, "2");
    p.put(TimeSeriesWorkload.TAG_CARDINALITY_PROPERTY, "1,2");
    p.put(CoreWorkload.INSERT_START_PROPERTY, "1451606400");
    p.put(TimeSeriesWorkload.DELAYED_SERIES_PROPERTY, "0");
    p.put(TimeSeriesWorkload.RANDOMIZE_TIMESERIES_ORDER_PROPERTY, "false");
    return p;
  }
  
  /** Helper to setup the workload for testing. */
  private TimeSeriesWorkload getWorkload(final Properties p, final boolean init) 
      throws WorkloadException {
    Measurements.setProperties(p);
    if (!init) {
      return new TimeSeriesWorkload();
    } else {
      final TimeSeriesWorkload workload = new TimeSeriesWorkload();
      workload.init(p);
      return workload;
    }
  }
  
  static class MockDB extends DB {
    final List<String> keys = new ArrayList<String>();
    final List<Map<String, ByteIterator>> values = 
        new ArrayList<Map<String, ByteIterator>>();
    
    @Override
    public Status read(String table, String key, Set<String> fields,
                       Map<String, ByteIterator> result) {
      return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status update(String table, String key,
        Map<String, ByteIterator> values) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status insert(String table, String key,
        Map<String, ByteIterator> values) {
      keys.add(key);
      this.values.add(values);
      return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
      // TODO Auto-generated method stub
      return Status.OK;
    }
    
    public void dumpStdout() {
      for (int i = 0; i < keys.size(); i++) {
        System.out.print("[" + i + "] Key: " + keys.get(i) + " Values: {");
        int x = 0;
        for (final Entry<String, ByteIterator> entry : values.get(i).entrySet()) {
          if (x++ > 0) {
            System.out.print(", ");
          }
          System.out.print("{" + entry.getKey() + " => ");
          if (entry.getKey().equals("YCSBV")) {
            System.out.print(new String(Utils.bytesToDouble(entry.getValue().toArray()) + "}"));  
          } else if (entry.getKey().equals("YCSBTS")) {
            System.out.print(new String(Utils.bytesToLong(entry.getValue().toArray()) + "}"));
          } else {
            System.out.print(new String(entry.getValue().toArray()) + "}");
          }
        }
        System.out.println("}");
      }
    }
  }
}