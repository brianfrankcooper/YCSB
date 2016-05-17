/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.stumbleupon.async.TimeoutException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.kududb.Type.STRING;

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
public class KuduYCSBClient extends com.yahoo.ycsb.DB {
  public static final String KEY = "key";
  public static final Status TIMEOUT =
      new Status("TIMEOUT", "The operation timed out.");
  public static final int MAX_TABLETS = 9000;
  public static final long DEFAULT_SLEEP = 60000;
  private static final String SYNC_OPS_OPT = "kudu_sync_ops";
  private static final String DEBUG_OPT = "kudu_debug";
  private static final String PRINT_ROW_ERRORS_OPT = "kudu_print_row_errors";
  private static final String PRE_SPLIT_NUM_TABLETS_OPT =
      "kudu_pre_split_num_tablets";
  private static final String TABLE_NUM_REPLICAS = "kudu_table_num_replicas";
  private static final String BLOCK_SIZE_OPT = "kudu_block_size";
  private static final String MASTER_ADDRESSES_OPT = "kudu_master_addresses";
  private static final int BLOCK_SIZE_DEFAULT = 4096;
  private static final List<String> COLUMN_NAMES = new ArrayList<String>();
  private static KuduClient client;
  private static Schema schema;
  private static int fieldCount;
  private boolean debug = false;
  private boolean printErrors = false;
  private String tableName;
  private KuduSession session;
  private KuduTable kuduTable;

  @Override
  public void init() throws DBException {
    if (getProperties().getProperty(DEBUG_OPT) != null) {
      this.debug = getProperties().getProperty(DEBUG_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors =
          getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors =
          getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    this.tableName = com.yahoo.ycsb.workloads.CoreWorkload.table;
    initClient(debug, tableName, getProperties());
    this.session = client.newSession();
    if (getProperties().getProperty(SYNC_OPS_OPT) != null
        && getProperties().getProperty(SYNC_OPS_OPT).equals("false")) {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
      this.session.setMutationBufferSpace(100);
    } else {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    }

    try {
      this.kuduTable = client.openTable(tableName);
    } catch (Exception e) {
      throw new DBException("Could not open a table because of:", e);
    }
  }

  private static synchronized void initClient(boolean debug, String tableName,
      Properties prop) throws DBException {
    if (client != null) {
      return;
    }

    String masterAddresses = prop.getProperty(MASTER_ADDRESSES_OPT);
    if (masterAddresses == null) {
      masterAddresses = "localhost:7051";
    }

    int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
    if (numTablets > MAX_TABLETS) {
      throw new DBException("Specified number of tablets (" + numTablets
          + ") must be equal " + "or below " + MAX_TABLETS);
    }

    int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, 3);

    int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);

    client = new KuduClient.KuduClientBuilder(masterAddresses)
        .defaultSocketReadTimeoutMs(DEFAULT_SLEEP)
        .defaultOperationTimeoutMs(DEFAULT_SLEEP)
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP).build();
    if (debug) {
      System.out.println("Connecting to the masters at " + masterAddresses);
    }

    fieldCount = getIntFromProp(prop, CoreWorkload.FIELD_COUNT_PROPERTY,
        Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(fieldCount + 1);

    ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
        .key(true).desiredBlockSize(blockSize).build();
    columns.add(keyColumn);
    COLUMN_NAMES.add(KEY);
    for (int i = 0; i < fieldCount; i++) {
      String name = "field" + i;
      COLUMN_NAMES.add(name);
      columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING)
          .desiredBlockSize(blockSize).build());
    }
    schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions();
    builder.setNumReplicas(numReplicas);
    // create n-1 split keys, which will end up being n tablets master-side
    for (int i = 1; i < numTablets + 0; i++) {
      // We do +1000 since YCSB starts at user1.
      int startKeyInt = (MAX_TABLETS / numTablets * i) + 1000;
      String startKey = String.format("%04d", startKeyInt);
      PartialRow splitRow = schema.newPartialRow();
      splitRow.addString(0, "user" + startKey);
      builder.addSplitRow(splitRow);
    }

    try {
      client.createTable(tableName, schema, builder);
    } catch (Exception e) {
      if (!e.getMessage().contains("ALREADY_PRESENT")) {
        throw new DBException("Couldn't create the table", e);
      }
    }
  }

  private static int getIntFromProp(Properties prop, String propName,
      int defaultValue) throws DBException {
    String intStr = prop.getProperty(propName);
    if (intStr == null) {
      return defaultValue;
    } else {
      try {
        return Integer.valueOf(intStr);
      } catch (NumberFormatException ex) {
        throw new DBException(
            "Provided number for " + propName + " isn't a valid integer");
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.session.close();
    } catch (Exception e) {
      throw new DBException("Couldn't cleanup the session", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results =
        new Vector<HashMap<String, ByteIterator>>();
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
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      KuduScanner.KuduScannerBuilder scannerBuilder =
          client.newScannerBuilder(this.kuduTable);
      List<String> querySchema;
      if (fields == null) {
        querySchema = COLUMN_NAMES;
        // No need to set the projected columns with the whole schema.
      } else {
        querySchema = new ArrayList<String>(fields);
        scannerBuilder.setProjectedColumnNames(querySchema);
      }

      KuduPredicate.ComparisonOp comparisonOp;
      if (recordcount == 1) {
        comparisonOp = KuduPredicate.ComparisonOp.EQUAL;
      } else {
        comparisonOp = KuduPredicate.ComparisonOp.GREATER_EQUAL;
      }
      KuduPredicate keyPredicate = KuduPredicate.newComparisonPredicate(
          schema.getColumnByIndex(0),
          comparisonOp,
          startkey);

      KuduScanner scanner = scannerBuilder
          .addPredicate(keyPredicate)
          .limit(recordcount) // currently noop
          .build();

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
      if (printErrors) {
        System.err.println(
            "Waited too long for a scan operation with start key=" + startkey);
      }
      return TIMEOUT;
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private void addAllRowsToResult(RowResultIterator it, int recordcount,
      List<String> querySchema, Vector<HashMap<String, ByteIterator>> result)
          throws Exception {
    RowResult row;
    HashMap<String, ByteIterator> rowResult =
        new HashMap<String, ByteIterator>(querySchema.size());
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
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    Update update = this.kuduTable.newUpdate();
    PartialRow row = update.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      String columnName = schema.getColumnByIndex(i).getName();
      if (values.containsKey(columnName)) {
        String value = values.get(columnName).toString();
        row.addString(columnName, value);
      }
    }
    apply(update);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    Insert insert = this.kuduTable.newInsert();
    PartialRow row = insert.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      row.addString(i, new String(
          values.get(schema.getColumnByIndex(i).getName()).toArray()));
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
      if (response != null && response.hasRowError() && printErrors) {
        System.err.println("Got a row error " + response.getRowError());
      }
    } catch (Exception ex) {
      if (printErrors) {
        System.err.println("Failed to apply an operation " + ex.toString());
        ex.printStackTrace();
      }
    }
  }
}
