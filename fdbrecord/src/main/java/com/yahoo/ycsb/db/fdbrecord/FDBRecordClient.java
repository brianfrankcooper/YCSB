/*
 * Copyright (c) 2015 - 2016, Yahoo!, Inc. All rights reserved.
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

package com.yahoo.ycsb.db.fdbrecord;

import com.apple.foundationdb.record.*;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.*;
import java.util.function.Function;

/**
 * FDB Record Layer client for YCSB framework.
 */

public class FDBRecordClient extends DB {

  private FDBDatabase db;
  private int batchSize;
  private int batchCount;
  private Vector<Message> batchRecords;
  private DynamicMessage.Builder msgBuilder;
  private Descriptors.Descriptor msgDesc;
  private Function<FDBRecordContext, FDBRecordStore> recordStoreProvider;

  private static final String CLUSTER_FILE = "fdb.clusterfile";
  private static final String CLUSTER_FILE_DEFAULT = "./fdb.cluster";
  private static final String KEY_SPACE_PATH_NAME = "fdb.keyspacepath";
  private static final String KEY_SPACE_PATH_DEFAULT = "YCSB";
  private static final String DB_BATCH_SIZE_DEFAULT = "0";
  private static final String DB_BATCH_SIZE = "fdb.batchsize";
  private static final String FIELD_COUNT = "fieldcount";
  private static final String FIELD_COUNT_DEFAULT = "10";
  private static final Logger logger = LoggerFactory.getLogger(FDBRecordClient.class);

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    final Properties props = getProperties();
    String clusterFile = props.getProperty(CLUSTER_FILE, CLUSTER_FILE_DEFAULT);
    String dbBatchSize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    String dbfieldCount = props.getProperty(FIELD_COUNT, FIELD_COUNT_DEFAULT);
    String keySpacePathName = props.getProperty(KEY_SPACE_PATH_NAME, KEY_SPACE_PATH_DEFAULT);

    KeySpace keySpace = new KeySpace(new KeySpaceDirectory(
        "YCSB-key-space",
        KeySpaceDirectory.KeyType.STRING,
        keySpacePathName
    ));
    KeySpacePath keySpacePath = keySpace.path("YCSB-key-space");

    DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
    schemaBuilder.setName("YCSB.proto");

    logger.info("Cluster File: {}\n", clusterFile);

    try {
      db = FDBDatabaseFactory.instance().getDatabase(clusterFile);
      batchSize = Integer.parseInt(dbBatchSize);
      int fieldCount = Integer.parseInt(dbfieldCount);
      batchCount = 0;
      batchRecords = new Vector<>(batchSize + 1);

      MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder("TOKEN");
      msgDefBuilder.addField("required", "string", "YCSB", 1);
      for (int i = 2; i < fieldCount + 2; i++) {
        msgDefBuilder.addField("required", "string", String.format("field%d", i - 2), i);
      }
      schemaBuilder.addMessageDefinition(msgDefBuilder.build());
      schemaBuilder.addMessageDefinition(MessageDefinition.newBuilder("RecordTypeUnion")
          .addField("optional", "TOKEN", "_TOKEN", 1)
          .build());
      DynamicSchema schema = schemaBuilder.build();

      msgBuilder = schema.newMessageBuilder("TOKEN");
      msgDesc = msgBuilder.getDescriptorForType();

      Descriptors.FileDescriptor fileDescriptor = msgDesc.getFile();

      RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
          .setRecords(fileDescriptor);

      metaDataBuilder.getRecordType("TOKEN")
          .setPrimaryKey(Key.Expressions.field("YCSB"));

      recordStoreProvider = context -> FDBRecordStore.newBuilder()
          .setMetaDataProvider(metaDataBuilder.getRecordMetaData())
          .setContext(context)
          .setKeySpacePath(keySpacePath)
          .createOrOpen();

    } catch (RecordCoreException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (batchCount > 0) {
      batchInsert();
      batchCount = 0;
    }
    try {
      db.close();
    } catch (RecordCoreException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "cleanup").getMessage(), e);
      throw new DBException(e);
    }
  }

  private Message getRecord(String key, Map<String, ByteIterator> values) {
    msgBuilder.clear();
    msgBuilder.setField(msgDesc.findFieldByName("YCSB"), key);
    for (Map.Entry<String, ByteIterator> ent : values.entrySet()) {
      msgBuilder.setField(msgDesc.findFieldByName(ent.getKey()), ent.getValue().toString());
    }
    return msgBuilder.build();
  }

  private Status convRecordToMap(Message record, Set<String> fields, Map<String, ByteIterator> result) {
    for (Map.Entry<Descriptors.FieldDescriptor, Object> ent : record.getAllFields().entrySet()) {
      String field = ent.getKey().getName();
      String value = (String) ent.getValue();
      result.put(field, new StringByteIterator(value));
    }
    if (fields != null) {
      for (String field : fields) {
        if (result.get(field) == null) {
          logger.debug("field not found: {}", field);
          return Status.NOT_FOUND;
        }
      }
    }
    return Status.OK;
  }

  private void batchInsert() {
    try {
      db.run(context -> {
          FDBRecordStore recordStore = recordStoreProvider.apply(context);

          for (int i = 0; i < batchCount; ++i) {
            recordStore.insertRecord(batchRecords.get(i));
          }

          return null;
        });
    } catch (Throwable e) {
      for (int i = 0; i < batchCount; ++i) {
        logger.error(MessageFormatter.format("Error batch inserting key {}", batchRecords.get(i)).getMessage(), e);
      }
    } finally {
      batchRecords.clear();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Message record = getRecord(key, values);
    String rowKey = (String) record.getField(msgDesc.findFieldByName("YCSB"));
    logger.debug("insert key = {}", rowKey);
    try {
      batchRecords.add(record);
      batchCount++;
      if (batchSize == 0 || batchSize == batchCount) {
        batchInsert();
        batchCount = 0;
      }
      return Status.OK;
    } catch (Throwable e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", rowKey).getMessage(), e);
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    Tuple rowKey = Tuple.from(key);
    logger.debug("delete key = {}", rowKey);
    try {
      db.run(context -> {
          FDBRecordStore recordStore = recordStoreProvider.apply(context);

          recordStore.deleteRecord(rowKey);
          return null;
        });
      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", rowKey).getMessage(), e);
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Tuple rowKey = Tuple.from(key);
    logger.debug("read key = {}", rowKey);
    try {
      FDBStoredRecord<Message> storedRecord = db.run(context ->
          // load the record
          recordStoreProvider.apply(context).loadRecord(rowKey)
        );
      if (storedRecord == null) {
        logger.debug("key not found: {}", rowKey);
        return Status.NOT_FOUND;
      }

      Message record = msgBuilder
          .mergeFrom(storedRecord.getRecord())
          .build();
      return convRecordToMap(record, fields, result);
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Tuple rowKey = Tuple.from(key);
    logger.debug("update key = {}", rowKey);
    try {
      return db.run(context -> {
          FDBRecordStore recordStore = recordStoreProvider.apply(context);

          FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(key));
          if (storedRecord == null) {
            logger.debug("key not found: {}", rowKey);
            return Status.NOT_FOUND;
          }
          Message.Builder originBuilder = storedRecord.getRecord().toBuilder();
          for (Map.Entry<String, ByteIterator> k : values.entrySet()) {
            String field = k.getKey();
            String value = k.getValue().toString();

            originBuilder.setField(msgDesc.findFieldByName(field), value);
          }

          Message record = recordStore.updateRecord(originBuilder.build()).getRecord();
          HashMap<String, ByteIterator> result = new HashMap<>();
          if (convRecordToMap(record, null, result) != Status.OK) {
            return Status.ERROR;
          }
          for (String k : values.keySet()) {
            if (result.containsKey(k)) {
              result.put(k, values.get(k));
            } else {
              logger.debug("field not fount: {}", k);
              return Status.NOT_FOUND;
            }
          }
          return Status.OK;
        });
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", rowKey).getMessage(), e);
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    Tuple startRowKey = Tuple.from(startkey);
    logger.debug("scan key from {} limit {}", startkey, recordcount);
    try {
      return db.run(context -> {
          FDBRecordStore recordStore = recordStoreProvider.apply(context);
          TupleRange range = TupleRange.between(startRowKey, null);
          for (FDBStoredRecord<Message> record :
              recordStore.scanRecords(range, null, ScanProperties.FORWARD_SCAN)
                  .limitRowsTo(recordcount)
                  .asList()
                  .join()) {
            final HashMap<String, ByteIterator> map = new HashMap<>();
            if (convRecordToMap(record.getRecord(), null, map) != Status.OK) {
              logger.error("Error scanning keys: from {} limit {}", startRowKey, recordcount);
              return Status.ERROR;
            }
            result.add(map);
          }
          return Status.OK;
        });
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} limit {} ",
          startRowKey, recordcount).getMessage(), e);
    }
    return Status.ERROR;
  }
}
