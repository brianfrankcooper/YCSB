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
/**
 * YDB binding for <a href="http://ydb.tech/">YDB</a>.
 *
 * See {@code ydb/README.md} for details.
 */
package site.ycsb.db.ydb;

import site.ycsb.*;
import tech.ydb.core.Result;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructValue;

/**
 * YDB client implementation.
 */
public class YDBClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(YDBClient.class);

  private static boolean usePreparedUpdateInsert = true;
  private static boolean forceUpsert = false;
  private static boolean useBulkUpsert = false;
  private static int bulkUpsertBatchSize = 1;

  private final List<Map<String, Value>> bulkBatch = new ArrayList<>();

  // YDB connection staff
  private YDBConnection connection;

  @Override
  public void init() throws DBException {
    LOGGER.debug("init ydb client");
    connection = YDBConnection.openConnection(getProperties());

    Properties properties = getProperties();

    usePreparedUpdateInsert = Boolean.parseBoolean(properties.getProperty("preparedInsertUpdateQueries", "true"));
    forceUpsert = Boolean.parseBoolean(properties.getProperty("forceUpsert", "false"));
    useBulkUpsert = Boolean.parseBoolean(properties.getProperty("bulkUpsert", "false"));
    bulkUpsertBatchSize = Integer.parseInt(properties.getProperty("bulkUpsertBatchSize", "1"));

    boolean isImport = Boolean.parseBoolean(properties.getProperty("import", "false"));
    if (isImport) {
      forceUpsert = true;
      useBulkUpsert = true;
      bulkUpsertBatchSize = 1000;
    }
  }

  @Override
  public void cleanup() throws DBException {
    LOGGER.debug("cleanup ydb client");

    if (!bulkBatch.isEmpty()) {
      YDBTable table = connection.tables().iterator().next();
      sendBulkBatch(table);
    }

    connection.close();
  }

  @Override
  public site.ycsb.Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    LOGGER.debug("read table {} with key {}", table, key);
    YDBTable ydbTable = connection.findTable(table);

    String fieldsString = "*";
    if (fields != null && !fields.isEmpty()) {
      fieldsString = String.join(",", fields);
    }
    String query = "DECLARE $key as Text; SELECT " + fieldsString + " FROM " + ydbTable.name()
        + " WHERE " + ydbTable.keyColumnName() + " = $key;";

    Params params = Params.of("$key", PrimitiveValue.newText(key));

    LOGGER.trace(query);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      Result<DataQueryResult> resultWrapped = connection.executeResult(
          session -> session.executeDataQuery(query, txControl, params))
          .join();
      resultWrapped.getStatus().expectSuccess("execute read query");
      DataQueryResult queryResult = resultWrapped.getValue();

      if (queryResult.getResultSetCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      ResultSetReader rs = queryResult.getResultSet(0);
      if (rs.getRowCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      final int keyColumnIndex = rs.getColumnIndex(ydbTable.keyColumnName());
      while (rs.next()) {
        for (int i = 0; i < rs.getColumnCount(); ++i) {
          if (i == keyColumnIndex) {
            final byte[] val = rs.getColumn(i).getText().getBytes();
            result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          } else {
            final byte[] val = rs.getColumn(i).getBytes();
            result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          }
        }
      }
    } catch (UnexpectedResultException e) {
      LOGGER.error(String.format("Select failed: %s", e.toString()));
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.debug("scan table {} from key {} and size {}", table, startkey, recordcount);
    YDBTable ydbTable = connection.findTable(table);

    String fieldsString = "*";
    if (fields != null && !fields.isEmpty()) {
      fieldsString = String.join(",", fields);
    }
    String query = "DECLARE $startKey as Text; DECLARE $limit as Uint32;"
        + " SELECT " + fieldsString + " FROM " + ydbTable.name()
        + " WHERE " + ydbTable.keyColumnName() + " >= $startKey"
        + " LIMIT $limit;";

    Params params = Params.of(
        "$startKey", PrimitiveValue.newText(startkey),
        "$limit", PrimitiveValue.newUint32(recordcount));

    LOGGER.trace(query);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      Result<DataQueryResult> resultWrapped = connection.executeResult(
          session -> session.executeDataQuery(query, txControl, params))
          .join();
      resultWrapped.getStatus().expectSuccess("execute scan query");
      DataQueryResult queryResult = resultWrapped.getValue();

      ResultSetReader rs = queryResult.getResultSet(0);
      final int keyColumnIndex = rs.getColumnIndex(ydbTable.keyColumnName());
      result.ensureCapacity(rs.getRowCount());
      while (rs.next()) {
        HashMap<String, ByteIterator> columns = new HashMap<>();
        for (int i = 0; i < rs.getColumnCount(); ++i) {
          if (i == keyColumnIndex) {
            final byte[] val = rs.getColumn(i).getText().getBytes();
            columns.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          } else {
            final byte[] val = rs.getColumn(i).getBytes();
            columns.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          }
        }
        result.add(columns);
      }
    } catch (UnexpectedResultException e) {
      LOGGER.error(String.format("Scan failed: %s", e.toString()));
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  private site.ycsb.Status executeQuery(String query, Params params, String op) {
    LOGGER.trace(query);

    try {
      TxControl txControl = TxControl.serializableRw().setCommitTx(true);
      connection.executeResult(session -> session.executeDataQuery(query, txControl, params))
          .join().getStatus().expectSuccess(String.format("execute %s query problem", op));
      return site.ycsb.Status.OK;
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }
  }

  private site.ycsb.Status insertOrUpdatePrepared(
      String table, String key, Map<String, ByteIterator> values, String op) {
    YDBTable ydbTable = connection.findTable(table);

    final StringBuilder queryDeclare = new StringBuilder();
    final StringBuilder queryColumns = new StringBuilder();
    final StringBuilder queryParams = new StringBuilder();
    final Params params = Params.create();

    queryDeclare.append("DECLARE $key AS Text;");
    queryColumns.append(ydbTable.keyColumnName());
    queryParams.append("$key");
    params.put("$key", PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        queryDeclare.append("DECLARE $").append(column).append(" AS Bytes;");
        queryColumns.append(", ").append(column);
        queryParams.append(", $").append(column);
        params.put("$" + column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    String query = queryDeclare.toString() + op + " INTO " + ydbTable.name()
        + " (" + queryColumns.toString() + " ) VALUES ( " + queryParams.toString() + ");";

    return executeQuery(query, params, op);
  }

  private site.ycsb.Status insertOrUpdateNotPrepared(
      String table, String key, Map<String, ByteIterator> values, String op) {
    YDBTable ydbTable = connection.findTable(table);

    final StringBuilder queryColumns = new StringBuilder();
    final StringBuilder queryValues = new StringBuilder();

    queryColumns.append(ydbTable.keyColumnName());
    queryValues.append("'").append(key).append("'");

    values.forEach((column, bytes) -> {
        queryColumns.append(", ").append(column);
        queryValues.append("'").append(bytes.toArray()).append("'");
      });

    String query = op + " INTO " + ydbTable.name()
        + " (" + queryColumns.toString() + " ) VALUES ( " + queryValues.toString() + ");";

    return executeQuery(query, Params.empty(), op);
  }

  private void sendBulkBatch(YDBTable ydbTable) {
    if (bulkBatch.isEmpty()) {
      return;
    }

    int bulkSize = bulkBatch.size();
    String tablePath = connection.getDatabase() + "/" + ydbTable.name();

    final Map<String, Type> ydbTypes = new HashMap<>();
    ydbTypes.put(ydbTable.keyColumnName(), PrimitiveType.Text);

    ydbTable.columnNames().forEach(column -> {
        ydbTypes.put(column, PrimitiveType.Bytes);
      });

    StructType type = StructType.of(ydbTypes);

    ListValue bulkData = ListType.of(type).newValue(
        bulkBatch.stream().map(type::newValue).collect(Collectors.toList())
    );
    bulkBatch.clear();

    try {
      connection.executeStatus(session -> session.executeBulkUpsert(tablePath, bulkData))
          .join().expectSuccess("bulk upsert problem for bulk size " + bulkSize);
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      throw e;
    }
  }

  private site.ycsb.Status bulkUpsertBatched(YDBTable ydbTable, String key, Map<String, ByteIterator> values) {
    Map<String, Value> ydbValues = new HashMap<>();
    ydbValues.put(ydbTable.keyColumnName(), PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        ydbValues.put(column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    bulkBatch.add(ydbValues);
    if (bulkBatch.size() < bulkUpsertBatchSize) {
      return site.ycsb.Status.BATCHED_OK;
    }

    sendBulkBatch(ydbTable);
    return site.ycsb.Status.OK;
  }

  private site.ycsb.Status bulkUpsert(String table, String key, Map<String, ByteIterator> values) {
    YDBTable ydbTable = connection.findTable(table);
    String tablePath = connection.getDatabase() + "/" + ydbTable.name();

    if (bulkUpsertBatchSize > 1) {
      return bulkUpsertBatched(ydbTable, key, values);
    }

    final Map<String, Type> ydbTypes = new HashMap<>();
    final Map<String, Value> ydbValues = new HashMap<>();

    ydbTypes.put(ydbTable.keyColumnName(), PrimitiveType.Text);
    ydbValues.put(ydbTable.keyColumnName(), PrimitiveValue.newText(key));

    values.forEach((column, bytes) -> {
        ydbTypes.put(column, PrimitiveType.Bytes);
        ydbValues.put(column, PrimitiveValue.newBytes(bytes.toArray()));
      });

    StructValue data = StructType.of(ydbTypes).newValue(ydbValues);

    try {
      connection.executeStatus(session -> session.executeBulkUpsert(tablePath, ListValue.of(data)))
          .join().expectSuccess("bulk upsert problem for key " + key);

      return site.ycsb.Status.OK;
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status update(String table, String key, Map<String, ByteIterator> values) {
    LOGGER.debug("update record table {} with key {}", table, key);
    // note that is is a blind update: i.e. we will never return NOT_FOUND
    if (usePreparedUpdateInsert) {
      if (useBulkUpsert) {
        return bulkUpsert(table, key, values);
      } else {
        return insertOrUpdatePrepared(table, key, values, "UPSERT");
      }
    } else {
      return insertOrUpdateNotPrepared(table, key, values, "UPSERT");
    }
  }

  @Override
  public site.ycsb.Status insert(String table, String key, Map<String, ByteIterator> values) {
    LOGGER.debug("insert record into table {} with key {}", table, key);
    // note that inserting same key twice results into error
    if (forceUpsert) {
      return update(table, key, values);
    }

    if (usePreparedUpdateInsert) {
      return insertOrUpdatePrepared(table, key, values, "INSERT");
    } else {
      return insertOrUpdateNotPrepared(table, key, values, "INSERT");
    }
  }

  @Override
  public site.ycsb.Status delete(String table, String key) {
    LOGGER.debug("delete record from table {} with key {}", table, key);
    YDBTable ydbTable = connection.findTable(table);

    String query = "DECLARE $key as Text; "
        + "DELETE from " + ydbTable.name()
        + " WHERE " + ydbTable.keyColumnName() + " = $key;";
    LOGGER.debug(query);

    Params params = Params.of("$key", PrimitiveValue.newText(key));

    try {
      TxControl txControl = TxControl.serializableRw().setCommitTx(true);
      StatusCode code = connection.executeResult(session -> session.executeDataQuery(query, txControl, params))
          .join().getStatus().getCode();

      switch (code) {
      case SUCCESS:
        return site.ycsb.Status.OK;
      case NOT_FOUND:
        return site.ycsb.Status.NOT_FOUND;
      default:
        return site.ycsb.Status.ERROR;
      }
    } catch (RuntimeException e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }
  }
}
