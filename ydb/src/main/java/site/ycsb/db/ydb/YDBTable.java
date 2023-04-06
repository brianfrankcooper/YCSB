/*
 * Copyright (c) 2022 YCSB contributors. All rights reserved.
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

package site.ycsb.db.ydb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.workloads.CoreWorkload;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.table.description.ColumnFamily;
import tech.ydb.table.description.StoragePool;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.PartitioningPolicy;
import tech.ydb.table.settings.PartitioningSettings;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.TupleValue;


/**
 * Helper for create YDB table and keep columns description.
 */
public class YDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(YDBTable.class);

  /** Key column name is 'key' (and type String). */
  private static final String KEY_COLUMN_NAME = "keyColumnName";
  private static final String KEY_COLUMN_NAME_DEFAULT = "id";

  private static final String KEY_DROP_ON_INIT = "dropOnInit";
  private static final String KEY_DROP_ON_INIT_DEFAULT = "false";

  private static final String KEY_DROP_ON_CLEAN = "dropOnClean";
  private static final String KEY_DROP_ON_CLEAN_DEFAULT = "false";

  private static final String KEY_DO_COMPRESSION = "compression";
  private static final String KEY_DO_COMPRESSION_DEFAULT = "";

  private static final String KEY_DO_PRESPLIT = "presplitTable";
  private static final String KEY_DO_PRESPLIT_DEFAULT = "";

  private static final String MAX_PARTITION_SIZE = "2000"; // 2 GB
  private static final String MAX_PARTITIONS_COUNT = "1000";

  private final String tableName;
  private final String keyColumnName;
  private final List<String> columnNames;
  private final TableDescription tableDescription;
  private final CreateTableSettings createTableSettings;

  private final boolean dropOnInit;
  private final boolean dropOnClean;

  public YDBTable(Properties props) {
    this.tableName = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    this.keyColumnName = props.getProperty(KEY_COLUMN_NAME, KEY_COLUMN_NAME_DEFAULT);

    this.columnNames = new ArrayList<>();
    String fieldPrefix = props.getProperty(CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    int fieldCount = Integer.parseInt(
        props.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT)
    );
    for (int i = 0; i < fieldCount; i++) {
      columnNames.add(fieldPrefix + i);
    }

    this.tableDescription = createTableDescription(props, keyColumnName, columnNames);
    this.createTableSettings = createTableSettings(props, this.tableDescription);
    this.dropOnInit = Boolean.parseBoolean(props.getProperty(KEY_DROP_ON_INIT, KEY_DROP_ON_INIT_DEFAULT));
    this.dropOnClean = Boolean.parseBoolean(props.getProperty(KEY_DROP_ON_CLEAN, KEY_DROP_ON_CLEAN_DEFAULT));
  }

  public String name() {
    return this.tableName;
  }

  public String keyColumnName() {
    return this.keyColumnName;
  }

  public List<String> columnNames() {
    return this.columnNames;
  }

  public void init(YDBConnection connection) throws DBException {
    if (!dropOnInit) {
      LOGGER.info("Skip table '{}' creation", tableName);
      return;
    }

    dropTable(connection);

    String tablePath = connection.getDatabase() + "/" + tableName;

    Status createStatus = connection.executeStatus(
        session -> session.createTable(
            tablePath,
            tableDescription,
            createTableSettings)).join();

    if (!createStatus.isSuccess()) {
      String msg = "Failed to create '" + tablePath + "': " + createStatus.toString();
      throw new DBException(msg);
    }

    LOGGER.info("Created table '{}' in database '{}'", tableName, connection.getDatabase());
  }

  public void clean(YDBConnection connection) throws DBException {
    if (!dropOnClean) {
      return;
    }

    dropTable(connection);
  }

  private void dropTable(YDBConnection connection) throws DBException {
    String tablePath = connection.getDatabase() + "/" + tableName;
    Status dropStatus = connection.executeStatus(session -> session.dropTable(tablePath)).join();
    if (!dropStatus.isSuccess() && dropStatus.getCode() != StatusCode.SCHEME_ERROR) {
      String msg = "Failed to drop '" + tablePath + "': " + dropStatus.toString();
      throw new DBException(msg);
    }
  }

  private static TableDescription createTableDescription(
      Properties props, String keyColumnName, List<String> columnNames) {
    String compressionDevice = props.getProperty(KEY_DO_COMPRESSION, KEY_DO_COMPRESSION_DEFAULT);

    TableDescription.Builder builder = TableDescription.newBuilder();
    String columnFamily = null;

    if (!compressionDevice.isEmpty()) {
      columnFamily = "default";
      StoragePool pool = new StoragePool(compressionDevice);
      ColumnFamily family = new ColumnFamily(columnFamily, pool, ColumnFamily.Compression.COMPRESSION_LZ4, false);
      builder.addColumnFamily(family);
    }

    builder.addNonnullColumn(keyColumnName, PrimitiveType.Text, columnFamily);
    for (String columnName: columnNames) {
      builder.addNullableColumn(columnName, PrimitiveType.Bytes, columnFamily);
    }
    builder.setPrimaryKey(keyColumnName);

    PartitioningSettings settings = new PartitioningSettings();

    final boolean splitByLoad = Boolean.parseBoolean(props.getProperty("splitByLoad", "true"));
    settings.setPartitioningByLoad(splitByLoad);

    final String maxPartsProp = props.getProperty("maxparts");
    int maxParts = 0;
    if (maxPartsProp != null) {
      maxParts = Integer.parseInt(maxPartsProp);
      settings.setMaxPartitionsCount(maxParts);
    }

    int threads = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    if (threads > 1) {
      int minParts = threads;
      if (maxParts != 0 && maxParts < minParts) {
        minParts = maxParts;
      }
      settings.setMinPartitionsCount(minParts);
    }

    final boolean splitBySize = Boolean.parseBoolean(props.getProperty("splitBySize", "true"));
    if (splitBySize) {
      int maxPartSizeMB = Integer.parseInt(props.getProperty("maxpartsizeMB", MAX_PARTITION_SIZE));
      settings.setPartitionSize(maxPartSizeMB);
      settings.setPartitioningBySize(true);
    }

    // set both until bug fixed
    builder.setPartitioningSettings(settings);

    return builder.build();
  }

  private static CreateTableSettings createTableSettings(Properties props, TableDescription description) {
    Long maxParts = description.getPartitioningSettings().getMaxPartitionsCount();
    int threads = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    final boolean dosplit = Boolean.parseBoolean(props.getProperty(KEY_DO_PRESPLIT, KEY_DO_PRESPLIT_DEFAULT));
    boolean hasManyParts = maxParts == null || maxParts == 0 || maxParts > 1;

    if (!dosplit || !hasManyParts || threads <= 1) {
      return new CreateTableSettings();
    }

    int rangecount = 0;
    if (maxParts != null) {
      rangecount = (int)maxParts.longValue();
    }
    if (rangecount == 0) {
      rangecount = threads;
    }
    // note that rangecount > 1

    final int zeropadding =
        Integer.parseInt(
            props.getProperty(CoreWorkload.ZERO_PADDING_PROPERTY, CoreWorkload.ZERO_PADDING_PROPERTY_DEFAULT));

    long recordcount =
        Long.parseLong(props.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

    boolean orderedinserts;
    final String orderedprop =
        props.getProperty(CoreWorkload.INSERT_ORDER_PROPERTY, CoreWorkload.INSERT_ORDER_PROPERTY_DEFAULT);
    if (orderedprop.compareTo("hashed") == 0) {
      orderedinserts = false;
    } else {
      orderedinserts = true;
    }

    LOGGER.info("Table will be presplitted into {} shards", rangecount);

    final long rangesize = recordcount / rangecount + 1;
    final int splitKeysSize = rangecount - 1;
    String[] splitKeys = new String[splitKeysSize];
    for (int i = 0; i < splitKeysSize; ++i) {
      long keynum = i * rangesize;
      splitKeys[i] = CoreWorkload.buildKeyName(keynum, zeropadding, orderedinserts);
    }

    if (!orderedinserts) {
      // keys are hashes, need to sort
      Arrays.sort(splitKeys);
    }

    PartitioningPolicy policy = new PartitioningPolicy();
    for (int i = 0; i < splitKeysSize; ++i) {
      policy.addExplicitPartitioningPoint(TupleValue.of(PrimitiveValue.newText(splitKeys[i]).makeOptional()));
    }

    CreateTableSettings settings = new CreateTableSettings();
    settings.setPartitioningPolicy(policy);

    return settings;
  }
}
