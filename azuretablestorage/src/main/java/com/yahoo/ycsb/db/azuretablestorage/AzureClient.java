/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db.azuretablestorage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.EntityResolver;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableServiceEntity;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


/**
 * YCSB binding for <a href="https://azure.microsoft.com/en-us/services/storage/">Azure</a>.
 * See {@code azure/README.md} for details.
 */
public class AzureClient extends DB {

  public static final String PROTOCOL = "azure.protocal";
  public static final String PROTOCOL_DEFAULT = "https";
  public static final String TABLE_ENDPOINT = "azure.endpoint";
  public static final String ACCOUNT = "azure.account";
  public static final String KEY = "azure.key";
  public static final String TABLE = "azure.table";
  public static final String TABLE_DEFAULT = "usertable";
  public static final String PARTITIONKEY = "azure.partitionkey";
  public static final String PARTITIONKEY_DEFAULT = "Test";
  public static final String BATCHSIZE = "azure.batchsize";
  public static final String BATCHSIZE_DEFAULT = "1";
  private static final int BATCHSIZE_UPPERBOUND = 100;
  private static final TableBatchOperation BATCH_OPERATION = new TableBatchOperation();
  private static String partitionKey;
  private CloudStorageAccount storageAccount = null;
  private CloudTableClient tableClient = null;
  private CloudTable cloudTable = null;
  private static int batchSize;
  private static int curIdx = 0;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String protocol = props.getProperty(PROTOCOL, PROTOCOL_DEFAULT);
    if (protocol != "https" && protocol != "http") {
      throw new DBException("Protocol must be 'http' or 'https'!\n");
    }
    String table = props.getProperty(TABLE, TABLE_DEFAULT);
    partitionKey = props.getProperty(PARTITIONKEY, PARTITIONKEY_DEFAULT);
    batchSize = Integer.parseInt(props.getProperty(BATCHSIZE, BATCHSIZE_DEFAULT));
    if (batchSize < 1 || batchSize > BATCHSIZE_UPPERBOUND) {
      throw new DBException(String.format("Batchsize must be between 1 and %d!\n", 
          BATCHSIZE_UPPERBOUND));
    }
    String account = props.getProperty(ACCOUNT);
    String key = props.getProperty(KEY);
    String tableEndPoint = props.getProperty(TABLE_ENDPOINT);
    String storageConnectionString = getStorageConnectionString(protocol, account, key, tableEndPoint);
    try {
      storageAccount = CloudStorageAccount.parse(storageConnectionString);
    } catch (Exception e)  {
      throw new DBException("Could not connect to the account.\n", e);
    }
    tableClient = storageAccount.createCloudTableClient();
    try {
      cloudTable = tableClient.getTableReference(table);
      cloudTable.createIfNotExists();
    } catch (Exception e)  {
      throw new DBException("Could not connect to the table.\n", e);
    }
  }

  @Override
  public void cleanup() {
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    if (fields != null) {
      return readSubset(key, fields, result);
    } else {
      return readEntity(key, result);
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, 
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      String whereStr = String.format("(PartitionKey eq '%s') and (RowKey ge '%s')", 
          partitionKey, startkey);
      TableQuery<DynamicTableEntity> scanQuery = 
          new TableQuery<DynamicTableEntity>(DynamicTableEntity.class)
          .where(whereStr).take(recordcount);
      int cnt = 0;
      for (DynamicTableEntity entity : cloudTable.execute(scanQuery)) {
        HashMap<String, EntityProperty> properties = entity.getProperties();
        HashMap<String, ByteIterator> cur = new HashMap<String, ByteIterator>();
        for (Entry<String, EntityProperty> entry : properties.entrySet()) {
          String fieldName = entry.getKey();
          ByteIterator fieldVal = new ByteArrayByteIterator(entry.getValue().getValueAsByteArray());
          if (fields == null || fields.contains(fieldName)) {
            cur.put(fieldName, fieldVal);
          }
        }
        result.add(cur);
        if (++cnt == recordcount) {
          break;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return insertOrUpdate(key, values);
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (batchSize == 1) {
      return insertOrUpdate(key, values);
    } else {
      return insertBatch(key, values);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      // firstly, retrieve the entity to be deleted
      TableOperation retrieveOp = 
          TableOperation.retrieve(partitionKey, key, TableServiceEntity.class);
      TableServiceEntity entity = cloudTable.execute(retrieveOp).getResultAsType();
      // secondly, delete the entity
      TableOperation deleteOp = TableOperation.delete(entity);
      cloudTable.execute(deleteOp);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  private String getStorageConnectionString(String protocol, String account, String key, String tableEndPoint) {
    String res = 
        String.format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s", 
        protocol, account, key);
    if (tableEndPoint != null) {
      res = String.format("%s;TableEndpoint=%s", res, tableEndPoint);
    }
    return res;
  }

  /*
   * Read subset of properties instead of full fields with projection.
   */
  public Status readSubset(String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    String whereStr = String.format("RowKey eq '%s'", key);

    TableQuery<TableServiceEntity> projectionQuery = TableQuery.from(
        TableServiceEntity.class).where(whereStr).select(fields.toArray(new String[0]));

    EntityResolver<HashMap<String, ByteIterator>> resolver = 
        new EntityResolver<HashMap<String, ByteIterator>>() {
          public HashMap<String, ByteIterator> resolve(String partitionkey, String rowKey, 
              Date timeStamp, HashMap<String, EntityProperty> properties, String etag) {
            HashMap<String, ByteIterator> tmp = new HashMap<String, ByteIterator>();
            for (Entry<String, EntityProperty> entry : properties.entrySet()) {
              String key = entry.getKey();
              ByteIterator val = new ByteArrayByteIterator(entry.getValue().getValueAsByteArray());
              tmp.put(key, val);
            }
            return tmp;
      }
    };
    try {
      for (HashMap<String, ByteIterator> tmp : cloudTable.execute(projectionQuery, resolver)) {
        for (Entry<String, ByteIterator> entry : tmp.entrySet()){
          String fieldName = entry.getKey();
          ByteIterator fieldVal = entry.getValue();
          result.put(fieldName, fieldVal);
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  private Status readEntity(String key, HashMap<String, ByteIterator> result) {
    try {
      // firstly, retrieve the entity to be deleted
      TableOperation retrieveOp = 
          TableOperation.retrieve(partitionKey, key, DynamicTableEntity.class);
      DynamicTableEntity entity = cloudTable.execute(retrieveOp).getResultAsType();
      HashMap<String, EntityProperty> properties = entity.getProperties();
      for (Entry<String, EntityProperty> entry: properties.entrySet()) {
        String fieldName = entry.getKey();
        ByteIterator fieldVal = new ByteArrayByteIterator(entry.getValue().getValueAsByteArray());
        result.put(fieldName, fieldVal);
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  private Status insertBatch(String key, HashMap<String, ByteIterator> values) {
    HashMap<String, EntityProperty> properties = new HashMap<String, EntityProperty>();
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      String fieldName = entry.getKey();
      byte[] fieldVal = entry.getValue().toArray();
      properties.put(fieldName, new EntityProperty(fieldVal));
    }
    DynamicTableEntity entity = new DynamicTableEntity(partitionKey, key, properties);
    BATCH_OPERATION.insertOrReplace(entity);    
    if (++curIdx == batchSize) {
      try {
        cloudTable.execute(BATCH_OPERATION);
        BATCH_OPERATION.clear();
        curIdx = 0;
      } catch (Exception e) {
        return Status.ERROR;
      }
    }
    return Status.OK;
  }
  
  private Status insertOrUpdate(String key, HashMap<String, ByteIterator> values) {
    HashMap<String, EntityProperty> properties = new HashMap<String, EntityProperty>();
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      String fieldName = entry.getKey();
      byte[] fieldVal = entry.getValue().toArray();
      properties.put(fieldName, new EntityProperty(fieldVal));
    }
    DynamicTableEntity entity = new DynamicTableEntity(partitionKey, key, properties);
    TableOperation insertOrReplace = TableOperation.insertOrReplace(entity);
    try {
      cloudTable.execute(insertOrReplace);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  
}
