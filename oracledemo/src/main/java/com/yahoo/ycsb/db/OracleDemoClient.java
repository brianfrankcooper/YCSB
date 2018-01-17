/**
 * Copyright (c) 2018 Oracle and/or its affiliates
 * 
 * The Universal Permissive License (UPL), Version 1.0
 *
 * https://oss.oracle.com/licenses/upl
 *
 */
package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.FaultException;

import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldRange;
import oracle.kv.table.TableIterator;

/**
 * YCSB binding for Oracle NoSQL Demo.
 */
public class OracleDemoClient extends DB {
  
  private KVStore store;
  private String storeName = "kvstore";
  private String[] endpoints = {"localhost:5000"};
  private static Logger logger = Logger.getLogger(OracleDemoClient.class);
  
  private String primaryKey = "KeyPrimary";
  
  private Set<String> fieldsAll = null;

  private TableAPI tableAPI = null;
  private Map<String, Table> tableInterfaceMap = new HashMap<String, Table>();

  public void init() throws DBException {
    Properties props = getProperties();
    
    String debug = props.getProperty("oracledemo.debug", "false");
    
    if (debug.equalsIgnoreCase("true")) {
      logger.setLevel(Level.DEBUG);
    }
    
    storeName = props.getProperty("oracledemo.store", storeName);
    
    endpoints[0] = props.getProperty("oracledemo.endpoint", endpoints[0]);
    endpoints = props.getProperty("oracledemo.endpoints", endpoints[0]).split(",");

    KVStoreConfig config = new KVStoreConfig(storeName, endpoints);
    
    try {
      this.store = KVStoreFactory.getStore(config);
      this.tableAPI = this.store.getTableAPI();
    } catch (FaultException e) {
      throw new DBException(e);
    }
    
    logger.info("oracle demo connection created with " + storeName + ", " +
        Arrays.toString(this.endpoints));    
  }

  public void cleanup() throws DBException {
    store.close();
    logger.info("oracle demo connection closed");    
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    logger.debug("readkey: " + key + " from table: " + table + " fields: " + fields);
    Table tableInterface = this.getTableInterface(table);
    PrimaryKey pkey = tableInterface.createPrimaryKey();
    pkey.put(primaryKey, key);
    Row row = tableAPI.get(pkey, null);
    this.rowToMap(row, fields, result);
    return Status.OK;
  }
  
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    logger.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table
        + " fields: " + fields);

    Table tableInterface = this.getTableInterface(table);
    PrimaryKey pkey = tableInterface.createPrimaryKey();

    final FieldRange range = tableInterface.createFieldRange(primaryKey).setStart(startkey, true);
    TableIterator<Row> iter = tableAPI.tableIterator(pkey, range.createMultiRowOptions(), null);

    try {
      while (iter.hasNext()) {
        HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        Row row = iter.next();
        this.rowToMap(row, fields, map);
        result.add(map);
        if (map.size() >= recordcount) {
          break;
        }
      }
    } finally {
      iter.close();
    }

    return Status.OK;
  }
  
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    logger.debug("updatekey: " + key + " into table: " + table); 
    Table tableInterface = this.getTableInterface(table);
    Row row = tableInterface.createRow();
    
    row.put(primaryKey, key);
    
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue().toString();
      row.put(k, v);
    } 
    
    tableAPI.put(row, null, null);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    logger.debug("insertkey: " + key + " into table: " + table);
    Table tableInterface = this.getTableInterface(table);
    Row row = tableInterface.createRow();
    
    row.put(primaryKey, key);
    
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue().toString();
      row.put(k, v);
    } 
    
    tableAPI.put(row, null, null);
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    logger.debug("deletekey: " + key + " from table: " + table);
    Table tableInterface = this.getTableInterface(table);
    PrimaryKey pkey = tableInterface.createPrimaryKey();
    pkey.put(primaryKey, key);
    tableAPI.delete(pkey, null, null);
    return Status.OK;
  }

  private Table getTableInterface(String table) {
    if (!tableInterfaceMap.containsKey(table)) {
      Table tableInterface = this.tableAPI.getTable(table);
      this.tableInterfaceMap.put(table, tableInterface);
    }
    return this.tableInterfaceMap.get(table);
  }

  private void rowToMap(Row row, Set<String> fields, Map<String, ByteIterator> map) {
    if (fields == null) {
  
      if (fieldsAll == null) {
        List<String> names = row.getFieldNames();
        fieldsAll = new HashSet<String>(names);
      }

      fields = fieldsAll;      
    }

    for (String col : fields) {
      FieldValue fv = row.get(col);
      String val = null;

      if (!fv.isNull()) {
        val = fv.asString().get();
      }

      logger.debug(String.format("Result- col: %s, val: %s", col, val));
      map.put(col, new StringByteIterator(val));
    }
  }
  
}
