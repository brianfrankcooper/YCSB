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
import com.yahoo.ycsb.StringByteIterator;
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
 * Kudu client for YCSB framework
 * Example to load:
 * $ ./bin/ycsb load kudu -P workloads/workloada  -threads 5
 * Example to run:
 * ./bin/ycsb run kudu -P workloads/workloada -p sync_ops=true -threads 5
 *
 */
public class KuduYCSBClient extends com.yahoo.ycsb.DB {
  public static final String KEY = "key";
  public static final int OK = 0;
  public static final int SERVER_ERROR = -1;
  public static final int NO_MATCHING_RECORD = -2;
  public static final int TIMEOUT = -3;
  public static final int FATAL_ERROR = -4;
  public static final int MAX_TABLETS = 9000;
  public static final long DEFAULT_SLEEP = 60000;
  private static final String SYNC_OPS_OPT = "sync_ops";
  private static final String DEBUG_OPT = "debug";
  private static final String PRINT_ROW_ERRORS_OPT = "print_row_errors";
  private static final String PRE_SPLIT_NUM_TABLETS_OPT = "pre_split_num_tablets";
  private static final String TABLE_NUM_REPLICAS = "table_num_replicas";
  private static final String BLOCK_SIZE_OPT = "block_size";
  private static final int BLOCK_SIZE_DEFAULT = 4096;
  private static final List<String> columnNames = new ArrayList<String>();
  private static KuduClient client;
  private static Schema schema;
  public boolean debug = false;
  public boolean printErrors = false;
  public String tableName;
  private KuduSession session;
  private KuduTable table;

  @Override
  public void init() throws DBException {
    if (getProperties().getProperty(DEBUG_OPT) != null) {
      this.debug = getProperties().getProperty(DEBUG_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors = getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    if (getProperties().getProperty(PRINT_ROW_ERRORS_OPT) != null) {
      this.printErrors = getProperties().getProperty(PRINT_ROW_ERRORS_OPT).equals("true");
    }
    this.tableName = com.yahoo.ycsb.workloads.CoreWorkload.table;
    initClient(debug, tableName, getProperties());
    this.session = client.newSession();
    if (getProperties().getProperty(SYNC_OPS_OPT) != null &&
        getProperties().getProperty(SYNC_OPS_OPT).equals("true")) {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    } else {
      this.session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    this.session.setMutationBufferSpace(100);
    try {
      this.table = client.openTable(tableName);
    } catch (Exception e) {
      throw new DBException("Could not open a table because of:", e);
    }
  }

  private synchronized static void initClient(boolean debug, String tableName, Properties prop)
      throws DBException {
    if (client != null) return;

    String masterQuorum = prop.getProperty("masterQuorum");
    if (masterQuorum == null) {
      masterQuorum = "localhost:7051";
    }

    int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
    if (numTablets > MAX_TABLETS) {
      throw new DBException("Specified number of tablets (" + numTablets + ") must be equal " +
          "or below " + MAX_TABLETS);
    }

    int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, 3);

    int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);

    client = new KuduClient.KuduClientBuilder(masterQuorum)
        .defaultSocketReadTimeoutMs(DEFAULT_SLEEP)
        .defaultOperationTimeoutMs(DEFAULT_SLEEP)
        .build();
    if (debug) {
      System.out.println("Connecting to the masters at " + masterQuorum);
    }

    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(11);

    ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
                             .key(true)
                             .desiredBlockSize(blockSize)
                             .build();
    columns.add(keyColumn);
    columnNames.add(KEY);
    for (int i = 0; i < 10; i++) {
      String name = "field" + i;
      columnNames.add(name);
      columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING)
                  .desiredBlockSize(blockSize)
                  .build());
    }
    schema = new Schema(columns);

    CreateTableBuilder builder = new CreateTableBuilder();
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

  private static int getIntFromProp(Properties prop, String propName, int defaultValue)
      throws DBException {
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
    } catch (Exception e) {
      throw new DBException("Couldn't cleanup the session", e);
    }
  }

  @Override
  public int read(String table, String key, Set<String> fields,
                  HashMap<String,ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    int ret = scan(table, key, 1, fields, results);
    if (ret != OK) return ret;
    if (results.size() != 1) return NO_MATCHING_RECORD;
    result.putAll(results.firstElement());
    return OK;
  }

  @Override
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
                  Vector<HashMap<String, ByteIterator>> result) {
    try {

      if (fields != null && fields.size() > 10) {
        System.err.println("YCSB doesn't expose the fields count to DBs and " +
            "more than 10 were requested");
        return FATAL_ERROR;
      }

      KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(this.table);
      List<String> querySchema;
      if (fields == null) {
        querySchema = columnNames;
        // No need to set the projected columns with the whole schema.
      } else {
        querySchema = new ArrayList<String>(fields);
        scannerBuilder.setProjectedColumnNames(querySchema);
      }

      PartialRow lowerBound = schema.newPartialRow();
      lowerBound.addString(0, startkey);
      scannerBuilder.lowerBound(lowerBound);
      if (recordcount == 1) {
        PartialRow upperBound = schema.newPartialRow();
        // Keys are fixed length, just adding something at the end is safe.
        upperBound.addString(0, startkey.concat(" "));
        scannerBuilder.exclusiveUpperBound(upperBound);
      }

      KuduScanner scanner = scannerBuilder
          .limit(recordcount) // currently noop
          .build();

      while (scanner.hasMoreRows()) {
        RowResultIterator data = scanner.nextRows();
        addAllRowsToResult(data, recordcount, querySchema, result);
        if (recordcount == result.size()) break;
      }
      RowResultIterator closer = scanner.close();
      addAllRowsToResult(closer, recordcount, querySchema, result);
    } catch (TimeoutException te) {
      if (printErrors) {
        System.err.println("Waited too long for a scan operation with start key=" + startkey);
      }
      return TIMEOUT;
    } catch (Exception e) {
      System.err.println("Unexpected exception " + e);
      e.printStackTrace();
      return SERVER_ERROR;
    }
    return OK;
  }

  private void addAllRowsToResult(RowResultIterator it, int recordcount,
                                  List<String> querySchema,
                                  Vector<HashMap<String, ByteIterator>> result)
      throws Exception {
    RowResult row;
    HashMap<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>(querySchema.size());
    if (it == null) return;
    while (it.hasNext()) {
      if (result.size() == recordcount) return;
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
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    Update update = this.table.newUpdate();
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
    return OK;
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    Insert insert = this.table.newInsert();
    PartialRow row = insert.getRow();
    row.addString(KEY, key);
    for (int i = 1; i < schema.getColumnCount(); i++) {
      row.addString(i, new String(values.get(schema.getColumnByIndex(i).getName()).toArray()));
    }
    apply(insert);
    return OK;
  }

  @Override
  public int delete(String table, String key) {
    Delete delete = this.table.newDelete();
    PartialRow row = delete.getRow();
    row.addString(KEY, key);
    apply(delete);
    return OK;
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
