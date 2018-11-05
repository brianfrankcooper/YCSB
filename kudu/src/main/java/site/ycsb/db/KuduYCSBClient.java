/**
 * Copyright (c) 2015-2016 YCSB contributors. All rights reserved.
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

package site.ycsb.db;

import com.stumbleupon.async.TimeoutException;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static site.ycsb.Client.DEFAULT_RECORD_COUNT;
import static site.ycsb.Client.RECORD_COUNT_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.INSERT_ORDER_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.INSERT_ORDER_PROPERTY_DEFAULT;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static site.ycsb.workloads.CoreWorkload.ZERO_PADDING_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.ZERO_PADDING_PROPERTY_DEFAULT;
import static org.apache.kudu.Type.STRING;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;

/**
 * Kudu client for YCSB framework. Example to load: <blockquote>
 *
 * <pre>
 * <code>
 * $ ./bin/ycsb load kudu -P workloads/workloada -threads 5
 * </code>
 * </pre>
 *
 * </blockquote> Example to run:  <blockquote>
 *
 * <pre>
 * <code>
 * ./bin/ycsb run kudu -P workloads/workloada -p kudu_sync_ops=true -threads 5
 * </code>
 * </pre>
 *
 * </blockquote>
 */
public class KuduYCSBClient extends site.ycsb.DB {
  private static final Logger LOG = LoggerFactory.getLogger(KuduYCSBClient.class);
  private static final String KEY = "key";
  private static final Status TIMEOUT = new Status("TIMEOUT", "The operation timed out.");
  private static final int MAX_TABLETS = 9000;
  private static final long DEFAULT_SLEEP = 60000;
  private static final int DEFAULT_NUM_CLIENTS = 1;
  private static final int DEFAULT_NUM_REPLICAS = 3;
  private static final String DEFAULT_PARTITION_SCHEMA = "hashPartition";

  private static final String SYNC_OPS_OPT = "kudu_sync_ops";
  private static final String BUFFER_NUM_OPS_OPT = "kudu_buffer_num_ops";
  private static final String PRE_SPLIT_NUM_TABLETS_OPT = "kudu_pre_split_num_tablets";
  private static final String TABLE_NUM_REPLICAS = "kudu_table_num_replicas";
  private static final String BLOCK_SIZE_OPT = "kudu_block_size";
  private static final String MASTER_ADDRESSES_OPT = "kudu_master_addresses";
  private static final String NUM_CLIENTS_OPT = "kudu_num_clients";
  private static final String PARTITION_SCHEMA_OPT = "kudu_partition_schema";

  private static final int BLOCK_SIZE_DEFAULT = 4096;
  private static final int BUFFER_NUM_OPS_DEFAULT = 2000;
  private static final List<String> COLUMN_NAMES = new ArrayList<>();

  private static List<KuduClient> clients = new ArrayList<>();
  private static int clientRoundRobin = 0;
  private static boolean tableSetup = false;
  private KuduClient client;
  private Schema schema;
  private String tableName;
  private KuduSession session;
  private KuduTable kuduTable;
  private String partitionSchema;
  private int zeropadding;
  private boolean orderedinserts;

  @Override
  public void init() throws DBException {
    Properties prop = getProperties();
    this.tableName = prop.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    this.partitionSchema = prop.getProperty(PARTITION_SCHEMA_OPT, DEFAULT_PARTITION_SCHEMA);
    this.zeropadding = Integer.parseInt(prop.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));
    if (prop.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      this.orderedinserts = false;
    } else {
      this.orderedinserts = true;
    }
    initClient();
    this.session = client.newSession();
    if (getProperties().getProperty(SYNC_OPS_OPT) != null
        && getProperties().getProperty(SYNC_OPS_OPT).equals("false")) {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
      this.session.setMutationBufferSpace(
          getIntFromProp(getProperties(), BUFFER_NUM_OPS_OPT, BUFFER_NUM_OPS_DEFAULT));
    } else {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    }

    try {
      this.kuduTable = client.openTable(tableName);
      this.schema = kuduTable.getSchema();
    } catch (Exception e) {
      throw new DBException("Could not open a table because of:", e);
    }
  }

  /**
   * Initialize the 'clients' member with the configured number of
   * clients.
   */
  private void initClients() throws DBException {
    synchronized (KuduYCSBClient.class) {
      if (!clients.isEmpty()) {
        return;
      }

      Properties prop = getProperties();

      String masterAddresses = prop.getProperty(MASTER_ADDRESSES_OPT,
                                                "localhost:7051");
      LOG.debug("Connecting to the masters at {}", masterAddresses);

      int numClients = getIntFromProp(prop, NUM_CLIENTS_OPT, DEFAULT_NUM_CLIENTS);
      for (int i = 0; i < numClients; i++) {
        clients.add(new KuduClient.KuduClientBuilder(masterAddresses)
                                  .defaultSocketReadTimeoutMs(DEFAULT_SLEEP)
                                  .defaultOperationTimeoutMs(DEFAULT_SLEEP)
                                  .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
                                  .build());
      }
    }
  }

  private void initClient() throws DBException {
    initClients();
    synchronized (clients) {
      client = clients.get(clientRoundRobin++ % clients.size());
    }
    setupTable();
  }

  private void setupTable() throws DBException {
    Properties prop = getProperties();
    synchronized (KuduYCSBClient.class) {
      if (tableSetup) {
        return;
      }
      int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
      if (numTablets > MAX_TABLETS) {
        throw new DBException("Specified number of tablets (" + numTablets
            + ") must be equal " + "or below " + MAX_TABLETS);
      }
      int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, DEFAULT_NUM_REPLICAS);
      long recordCount = Long.parseLong(prop.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
      if (recordCount == 0) {
        recordCount = Integer.MAX_VALUE;
      }
      int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);
      int fieldCount = getIntFromProp(prop, CoreWorkload.FIELD_COUNT_PROPERTY,
                                      Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      final String fieldprefix = prop.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                  CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

      List<ColumnSchema> columns = new ArrayList<ColumnSchema>(fieldCount + 1);

      ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
                                               .key(true)
                                               .desiredBlockSize(blockSize)
                                               .build();
      columns.add(keyColumn);
      COLUMN_NAMES.add(KEY);
      for (int i = 0; i < fieldCount; i++) {
        String name = fieldprefix + i;
        COLUMN_NAMES.add(name);
        columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING)
                                    .desiredBlockSize(blockSize)
                                    .build());
      }
      schema = new Schema(columns);

      CreateTableOptions builder = new CreateTableOptions();

      if (partitionSchema.equals("hashPartition")) {
        builder.setRangePartitionColumns(new ArrayList<String>());
        List<String> hashPartitionColumns = new ArrayList<>();
        hashPartitionColumns.add(KEY);
        builder.addHashPartitions(hashPartitionColumns, numTablets);
      } else if (partitionSchema.equals("rangePartition")) {
        if (!orderedinserts) {
          // We need to use ordered keys to determine how to split range partitions.
          throw new DBException("Must specify `insertorder=ordered` if using rangePartition schema.");
        }

        String maxKeyValue = String.valueOf(recordCount);
        if (zeropadding < maxKeyValue.length()) {
          throw new DBException(String.format("Invalid zeropadding value: %d, zeropadding needs to be larger "
              + "or equal to number of digits in the record number: %d.", zeropadding, maxKeyValue.length()));
        }

        List<String> rangePartitionColumns = new ArrayList<>();
        rangePartitionColumns.add(KEY);
        builder.setRangePartitionColumns(rangePartitionColumns);
        // Add rangePartitions
        long lowerNum = 0;
        long upperNum = 0;
        int remainder = (int) recordCount % numTablets;
        for (int i = 0; i < numTablets; i++) {
          lowerNum = upperNum;
          upperNum = lowerNum + recordCount / numTablets;
          if (i < remainder) {
            ++upperNum;
          }
          PartialRow lower = schema.newPartialRow();
          lower.addString(KEY, CoreWorkload.buildKeyName(lowerNum, zeropadding, orderedinserts));
          PartialRow upper = schema.newPartialRow();
          upper.addString(KEY, CoreWorkload.buildKeyName(upperNum, zeropadding, orderedinserts));
          builder.addRangePartition(lower, upper);
        }
      } else {
        throw new DBException("Invalid partition_schema specified: " + partitionSchema
            + ", must specify `partition_schema=hashPartition` or `partition_schema=rangePartition`");
      }
      builder.setNumReplicas(numReplicas);

      try {
        client.createTable(tableName, schema, builder);
      } catch (Exception e) {
        if (!e.getMessage().contains("already exists")) {
          throw new DBException("Couldn't create the table", e);
        }
      }
      tableSetup = true;
    }
  }

  private static int getIntFromProp(Properties prop,
                                    String propName,
                                    int defaultValue) throws DBException {
    String intStr = prop.getProperty(propName);
    if (intStr == null) {
      return defaultValue;
    } else {
      try {
        return Integer.valueOf(intStr);
      } catch (NumberFormatException ex) {
        throw new DBException("Provided number for " + propName + " isn't a valid integer");
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.session.close();
      this.client.close();
    } catch (Exception e) {
      throw new DBException("Couldn't cleanup the session", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    final Status status = scan(table, key, 1, fields, results);
    if (!status.equals(Status.OK)) {
      return status;
    }
    if (results.size() != 1) {
      return Status.NOT_FOUND;
    }
    result.putAll(results.firstElement());
    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kuduTable);
      List<String> querySchema;
      if (fields == null) {
        querySchema = COLUMN_NAMES;
        // No need to set the projected columns with the whole schema.
      } else {
        querySchema = new ArrayList<>(fields);
        scannerBuilder.setProjectedColumnNames(querySchema);
      }

      ColumnSchema column = schema.getColumnByIndex(0);
      KuduPredicate.ComparisonOp predicateOp = recordcount == 1 ? EQUAL : GREATER_EQUAL;
      KuduPredicate predicate = KuduPredicate.newComparisonPredicate(column, predicateOp, startkey);
      scannerBuilder.addPredicate(predicate);
      scannerBuilder.limit(recordcount); // currently noop

      KuduScanner scanner = scannerBuilder.build();

      while (scanner.hasMoreRows()) {
        RowResultIterator data = scanner.nextRows();
        addAllRowsToResult(data, recordcount, querySchema, result);
        if (recordcount == result.size()) {
          break;
        }
      }
      RowResultIterator closer = scanner.close();
      addAllRowsToResult(closer, recordcount, querySchema, result);
    } catch (TimeoutException te) {
      LOG.info("Waited too long for a scan operation with start key={}", startkey);
      return TIMEOUT;
    } catch (Exception e) {
      LOG.warn("Unexpected exception", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void addAllRowsToResult(RowResultIterator it,
                                  int recordcount,
                                  List<String> querySchema,
                                  Vector<HashMap<String, ByteIterator>> result) throws Exception {
    RowResult row;
    HashMap<String, ByteIterator> rowResult = new HashMap<>(querySchema.size());
    if (it == null) {
      return;
    }
    while (it.hasNext()) {
      if (result.size() == recordcount) {
        return;
      }
      row = it.next();
      int colIdx = 0;
      for (String col : querySchema) {
        rowResult.put(col, new StringByteIterator(row.getString(colIdx)));
        colIdx++;
      }
      result.add(rowResult);
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Update update = this.kuduTable.newUpdate();
    PartialRow row = update.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      String columnName = schema.getColumnByIndex(i).getName();
      ByteIterator b = values.get(columnName);
      if (b != null) {
        row.addStringUtf8(columnName, b.toArray());
      }
    }
    apply(update);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Insert insert = this.kuduTable.newInsert();
    PartialRow row = insert.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      row.addStringUtf8(i, values.get(schema.getColumnByIndex(i).getName()).toArray());
    }
    apply(insert);
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    Delete delete = this.kuduTable.newDelete();
    PartialRow row = delete.getRow();
    row.addString(KEY, key);
    apply(delete);
    return Status.OK;
  }

  private void apply(Operation op) {
    try {
      OperationResponse response = session.apply(op);
      if (response != null && response.hasRowError()) {
        LOG.info("Write operation failed: {}", response.getRowError());
      }
    } catch (KuduException ex) {
      LOG.warn("Write operation failed", ex);
    }
  }
}
