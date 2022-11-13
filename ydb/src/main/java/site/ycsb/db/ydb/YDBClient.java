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
import site.ycsb.workloads.CoreWorkload;

import com.yandex.ydb.auth.iam.CloudAuthHelper;
import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.StatusCode;
import com.yandex.ydb.core.UnexpectedResultException;
import com.yandex.ydb.core.auth.AuthProvider;
import com.yandex.ydb.core.auth.TokenAuthProvider;
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.table.SessionRetryContext;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.description.ColumnFamily;
import com.yandex.ydb.table.description.StoragePool;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.query.Params;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import com.yandex.ydb.table.settings.BulkUpsertSettings;
import com.yandex.ydb.table.settings.CreateTableSettings;
import com.yandex.ydb.table.settings.ExecuteDataQuerySettings;
import com.yandex.ydb.table.settings.PartitioningSettings;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.ListValue;
import com.yandex.ydb.table.values.PrimitiveType;
import com.yandex.ydb.table.values.PrimitiveValue;
import com.yandex.ydb.table.values.StructType;
import com.yandex.ydb.table.values.Type;
import com.yandex.ydb.table.values.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * YDB client implementation.
 */
public class YDBClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(YDBClient.class);

  /** Key column name is 'key' (and type String). */
  private static final String KEY_COLUMN_NAME = "key";
  private static final String VALUE_COLUMN_NAME = "value";

  private static final String MAX_PARTITION_SIZE = "2000"; // 2 GB
  private static final String MAX_PARTITIONS_COUNT = "50";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static String tablename;
  private static boolean usePreparedUpdateInsert = true;
  private static boolean forceUpsert = false;
  private static boolean useBulkUpsert = false;
  private static boolean useSingleColumn = false;
  private static int insertInflight = 1;

  private final AtomicInteger insertInflightLeft = new AtomicInteger(1);

  // YDB connection staff
  private String database;
  private TableClient tableclient;
  private SessionRetryContext retryctx;

  // from RocksDBClient.java
  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  // from RocksDBClient.java
  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private void dropTable() throws DBException {
    Status dropstatus =
        this.retryctx.supplyStatus(session -> session.dropTable(this.database + "/" + tablename)).join();
    if (dropstatus.getCode() != StatusCode.SUCCESS
        && dropstatus.getCode() != StatusCode.NOT_FOUND
        && dropstatus.getCode() != StatusCode.SCHEME_ERROR) {
      String msg = "Failed to drop '" + tablename + "': " + dropstatus.toString();
      throw new DBException(msg);
    }
  }

  private int calculateAvgRowSize() {
    Properties properties = getProperties();

    int fieldlength = Integer.parseInt(properties.getProperty(
        CoreWorkload.FIELD_LENGTH_PROPERTY, CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));

    int minfieldlength = Integer.parseInt(properties.getProperty(
        CoreWorkload.MIN_FIELD_LENGTH_PROPERTY, CoreWorkload.MIN_FIELD_LENGTH_PROPERTY_DEFAULT));

    String fieldlengthdistribution = properties.getProperty(
        CoreWorkload.FIELD_LENGTH_DISTRIBUTION_PROPERTY, CoreWorkload.FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    int avgFieldLength = 0;
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      avgFieldLength = fieldlength;
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      if (minfieldlength < fieldlength) {
        avgFieldLength = (fieldlength - minfieldlength) / 2 + 1;
      } else {
        avgFieldLength = fieldlength / 2 + 1;
      }
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      avgFieldLength = fieldlength / 4 + 1;
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      // TODO: properly handle this case, for now just some value
      avgFieldLength = fieldlength;
    } else {
      avgFieldLength = fieldlength;
    }

    int fieldcount = Integer.parseInt(properties.getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    return avgFieldLength * fieldcount;
  }

  private void createTable() throws DBException {
    Properties properties = getProperties();

    final boolean doDrop = Boolean.parseBoolean(properties.getProperty("dropOnInit", "false"));
    if (doDrop) {
      dropTable();
    }

    final boolean doCompression = Boolean.parseBoolean(properties.getProperty("compression", "false"));

    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

    int fieldcount = Integer.parseInt(properties.getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    TableDescription.Builder builder = TableDescription.newBuilder();

    if (doCompression) {
      StoragePool pool = new StoragePool("ssd"); // TODO: must be from opts
      ColumnFamily family = new ColumnFamily("default", pool, ColumnFamily.Compression.COMPRESSION_LZ4, false);
      builder.addColumnFamily(family);
    }

    if (doCompression) {
      builder.addNullableColumn(KEY_COLUMN_NAME, PrimitiveType.utf8(), "default");
    } else {
      builder.addNullableColumn(KEY_COLUMN_NAME, PrimitiveType.utf8());
    }

    if (!useSingleColumn) {
      for (int i = 0; i < fieldcount; i++) {
        if (doCompression) {
          builder.addNullableColumn(fieldprefix + i, PrimitiveType.utf8(), "default");
        } else {
          builder.addNullableColumn(fieldprefix + i, PrimitiveType.utf8());
        }
      }
    } else {
      if (doCompression) {
        builder.addNullableColumn(VALUE_COLUMN_NAME, PrimitiveType.utf8(), "default");
      } else {
        builder.addNullableColumn(VALUE_COLUMN_NAME, PrimitiveType.utf8());
      }
    }

    builder.setPrimaryKey(KEY_COLUMN_NAME);

    final boolean autopartitioning = Boolean.parseBoolean(properties.getProperty("autopartitioning", "true"));
    CreateTableSettings tableSettings = new CreateTableSettings();
    if (autopartitioning) {
      int avgRowSize = calculateAvgRowSize();
      long recordcount = Long.parseLong(properties.getProperty(
          Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

      int maxPartSizeMB = Integer.parseInt(properties.getProperty("maxpartsizeMB", MAX_PARTITION_SIZE));
      int maxParts = Integer.parseInt(properties.getProperty("maxparts", MAX_PARTITIONS_COUNT));
      long minParts = maxParts;

      long approximateDataSize = avgRowSize * recordcount;
      long avgPartSizeMB = Math.max(approximateDataSize / maxParts / 1000000, 1);
      long partSizeMB = Math.min(avgPartSizeMB, maxPartSizeMB);

      final boolean splitByLoad = Boolean.parseBoolean(properties.getProperty("splitByLoad", "true"));
      final boolean splitBySize = Boolean.parseBoolean(properties.getProperty("splitBySize", "true"));

      LOGGER.info(String.format(
          "After partitioning for %d records with avg row size %d: " +
          "minParts=%d, maxParts=%d, partSize=%d MB, " +
          "splitByLoad=%b, splitBySize=%b",
          recordcount, avgRowSize, minParts, maxParts, partSizeMB, splitByLoad, splitBySize));

      PartitioningSettings settings = new PartitioningSettings();
      settings.setMinPartitionsCount(minParts);
      settings.setMaxPartitionsCount(maxParts);
      settings.setPartitioningByLoad(splitByLoad);

      if (splitBySize) {
        settings.setPartitionSize(partSizeMB);
        settings.setPartitioningBySize(true);
      } else {
        settings.setPartitioningBySize(true);
      }

      // set both until bug fixed
      builder.setPartitioningSettings(settings);
      tableSettings.setPartitioningSettings(settings);
    }

    TableDescription tabledescription = builder.build();
    try {
      String tablepath = this.database + "/" + tablename;
      this.retryctx.supplyStatus(session -> session.createTable(tablepath, tabledescription, tableSettings))
        .join().expect("create table problem");
    } catch (UnexpectedResultException e) {
      throw new DBException(e);
    } finally {
      LOGGER.info(String.format("Created table '%s' in database '%s'", tablename, this.database));
    }
  }

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    Properties properties = getProperties();

    tablename = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    usePreparedUpdateInsert = Boolean.parseBoolean(properties.getProperty("preparedInsertUpdateQueries", "true"));
    forceUpsert = Boolean.parseBoolean(properties.getProperty("forceUpsert", "false"));
    useBulkUpsert = Boolean.parseBoolean(properties.getProperty("bulkUpsert", "false"));
    useSingleColumn = Boolean.parseBoolean(properties.getProperty("singleColumn", "false"));

    insertInflight = Integer.parseInt(properties.getProperty("insertInflight", "1"));
    if (insertInflight > 1) {
      insertInflightLeft.set(insertInflight);
    }

    String url = properties.getProperty("endpoint", null);
    if (url == null) {
      throw new DBException("ERROR: Missing endpoint");
    }

    if (!url.startsWith("grpc")) {
      throw new DBException("Invalid endpoint: '" + url + ";. Must be of the form 'grpc[s]://url:port'");
    }

    String databasepath = properties.getProperty("database", null);
    if (databasepath == null) {
      throw new DBException("ERROR: Missing database");
    }

    String connectionString = url + "?database=" + databasepath;
    LOGGER.info("YDB connection string: " + connectionString);

    String token = properties.getProperty("token", "");

    AuthProvider authProvider;
    if (token.isEmpty()) {
      authProvider = CloudAuthHelper.getAuthProviderFromEnviron();
    } else {
      authProvider = new TokenAuthProvider(token);
    }

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
        .withAuthProvider(authProvider)
        .build();

    GrpcTableRpc rpc = GrpcTableRpc.ownTransport(transport);
    this.tableclient = TableClient.newClient(rpc)
        .sessionPoolSize(insertInflight, insertInflight)
        .build();

    this.database = transport.getDatabase();
    this.retryctx = SessionRetryContext.create(this.tableclient).build();

    this.createTable();
  }

  @Override
  public void cleanup() throws DBException {
    while (insertInflightLeft.get() != insertInflight) {
      // wait
    }

    if (INIT_COUNT.decrementAndGet() != 0) {
      return;
    }

    // last instance

    Properties properties = getProperties();
    final boolean doDrop = Boolean.parseBoolean(properties.getProperty("dropOnClean", "false"));
    if (doDrop) {
      dropTable();
    }
  }

  @Override
  public site.ycsb.Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String query;

    if (!useSingleColumn) {
      String fieldsString = "*";
      if (fields != null && fields.size() > 0) {
        fieldsString = String.join(",", fields);
      }
      query = "DECLARE $key as Utf8; SELECT " + fieldsString + " FROM " + tablename + " WHERE key = $key;";
    } else {
      query = "DECLARE $key as Utf8; SELECT * FROM " + tablename + " WHERE key = $key;";
    }

    Params params = Params.of("$key", PrimitiveValue.utf8(key));

    LOGGER.debug(query);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      ExecuteDataQuerySettings executeSettings = new ExecuteDataQuerySettings().keepInQueryCache();
      DataQueryResult queryResult = this.retryctx.supplyResult(
          session -> session.executeDataQuery(query, txControl, params, executeSettings))
            .join().expect("execute read query");

      if (queryResult.getResultSetCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      ResultSetReader rs = queryResult.getResultSet(0);
      if (rs.getRowCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      while (rs.next()) {
        if (!useSingleColumn) {
          for (int i = 0; i < rs.getColumnCount(); ++i) {
            final byte[] val = rs.getColumn(i).getUtf8().getBytes();
            result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
          }
        } else {
          final byte[] readKey = rs.getColumn(0).getUtf8().getBytes();
          result.put(rs.getColumnName(0), new ByteArrayByteIterator(readKey));

          final byte[] readValue = rs.getColumn(1).getUtf8().getBytes();
          byte[] decodedValue = Base64.getDecoder().decode(readValue);
          deserializeValues(decodedValue, fields, result);
        }
      }
    } catch (Exception e) {
      LOGGER.error(String.format("Select failed: %s", e.toString()));
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String fieldsString = "*";
    if (fields != null && fields.size() > 0) {
      fieldsString = String.join(",", fields);
    }
    String query = "DECLARE $startKey as Utf8; DECLARE $limit as Uint32; SELECT " + fieldsString + " FROM " + tablename
        + " WHERE key >= $startKey"
        + " LIMIT $limit;";

    Params params = Params.of(
        "$startKey", PrimitiveValue.utf8(startkey),
        "$limit", PrimitiveValue.uint32(recordcount));

    LOGGER.debug(query);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      ExecuteDataQuerySettings executeSettings = new ExecuteDataQuerySettings().keepInQueryCache();
      DataQueryResult queryResult = this.retryctx.supplyResult(
          session -> session.executeDataQuery(query, txControl, params, executeSettings))
          .join().expect("execute scan query");

      ResultSetReader rs = queryResult.getResultSet(0);
      result.ensureCapacity(rs.getRowCount());
      while (rs.next()) {
        HashMap<String, ByteIterator> columns = new HashMap<String, ByteIterator>();
        for (int i = 0; i < rs.getColumnCount(); ++i) {
          final byte[] val = rs.getColumn(i).getUtf8().getBytes();
          columns.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
        }
        result.add(columns);
      }
    } catch (Exception e) {
      LOGGER.error(String.format("Scan failed: %s", e.toString()));
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  private site.ycsb.Status executeQuery(String query, Params params, String op) {
    LOGGER.debug(query);

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      ExecuteDataQuerySettings executeSettings = new ExecuteDataQuerySettings().keepInQueryCache();

      insertInflightLeft.decrementAndGet();
      CompletableFuture<Result<DataQueryResult>> future = this.retryctx.supplyResult(
          session -> session.executeDataQuery(query, txControl, params, executeSettings));

      if (insertInflight <= 1) {
        future.join().expect(String.format("execute %s query problem", op));
        insertInflightLeft.incrementAndGet();
      } else {
        future.thenAccept(result -> {
            if (result.getCode() != StatusCode.SUCCESS) {
              LOGGER.error(String.format("Operation failed: %s", result.toString()));
            }
          }).thenRun(() -> insertInflightLeft.incrementAndGet());
        while (insertInflightLeft.get() == 0) {
          Thread.sleep(1);
        }
      }

      return site.ycsb.Status.OK;
    } catch (Exception e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }
  }

  private site.ycsb.Status insertOrUpdatePreparedSingleColumn(
                      String table, String key, Map<String, ByteIterator> values, String op) {
    // we assume that for the same map of the same fields the order will be the same
    StringBuilder sb = new StringBuilder();

    sb.append("DECLARE $key AS Utf8;");
    sb.append("DECLARE $value AS Utf8;");

    sb.append(op);
    sb.append(" INTO ");
    sb.append(tablename);

    sb.append(" ( key, value ) VALUES ( $key, $value );");

    Params params = Params.create();
    params.put("$" + KEY_COLUMN_NAME, PrimitiveValue.utf8(key));

    try {
      String encoded = Base64.getEncoder().encodeToString(serializeValues(values));
      params.put("$" + VALUE_COLUMN_NAME, PrimitiveValue.utf8(encoded));
    } catch (Exception e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }

    String query = sb.toString();
    return executeQuery(query, params, op);
  }

  private site.ycsb.Status insertOrUpdatePrepared(
                      String table, String key, Map<String, ByteIterator> values, String op) {
    if (useSingleColumn) {
      return insertOrUpdatePreparedSingleColumn(table, key, values, op);
    }

    // we assume that for the same map of the same fields the order will be the same
    StringBuilder sb = new StringBuilder();

    sb.append("DECLARE $key AS Utf8;");
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      sb.append("DECLARE $");
      sb.append(entry.getKey());
      sb.append(" AS Utf8;");
    }

    sb.append(op);
    sb.append(" INTO ");
    sb.append(tablename);

    sb.append(" ( key, ");
    int n = values.size();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      --n;
      sb.append(entry.getKey());
      if (n != 0) {
        sb.append(", ");
      }
    }

    sb.append(") VALUES ( $key, ");
    n = values.size();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      --n;
      sb.append("$");
      sb.append(entry.getKey());
      if (n != 0) {
        sb.append(", ");
      }
    }
    sb.append(");");

    Params params = Params.create();
    params.put("$" + KEY_COLUMN_NAME, PrimitiveValue.utf8(key));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      params.put("$" + entry.getKey(), PrimitiveValue.utf8(entry.getValue().toString()));
    }

    String query = sb.toString();
    return executeQuery(query, params, op);
  }

  private site.ycsb.Status insertOrUpdateNotPrepared(
                      String table, String key, Map<String, ByteIterator> values, String op) {
    // Note that it doesn't use prepared queries, which is bad practice. Implemented only to compare performance
    // of prepared VS not prepared
    Set<String> fields = values.keySet();
    String fieldsString = KEY_COLUMN_NAME + "," + String.join(",", fields);

    StringBuilder sb = new StringBuilder(op + " INTO " + tablename
        + " (" + fieldsString + ") VALUES ('" + key + "',");
    int i = 0;
    int last = values.size() - 1;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      // note that using byte array we avoid escaping
      sb.append("'" + entry.getValue().toArray() + "'");
      if (i != last) {
        sb.append(",");
      }
      ++i;
    }
    sb.append(");");

    String query = sb.toString();
    LOGGER.debug(query);

    return executeQuery(query, Params.empty(), op);
  }

  private site.ycsb.Status bulkUpsert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, Type> types = new HashMap<String, Type>();
    types.put(KEY_COLUMN_NAME, PrimitiveType.utf8());

    Map<String, Value> ydbValues = new HashMap<String, Value>();
    ydbValues.put(KEY_COLUMN_NAME, PrimitiveValue.utf8(key));

    if (!useSingleColumn) {
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        types.put(entry.getKey(), PrimitiveType.utf8());
        ydbValues.put(entry.getKey(), PrimitiveValue.utf8(entry.getValue().toString()));
      }
    } else {
      types.put(VALUE_COLUMN_NAME, PrimitiveType.utf8());
      try {
        String encoded = Base64.getEncoder().encodeToString(serializeValues(values));
        ydbValues.put(VALUE_COLUMN_NAME, PrimitiveValue.utf8(encoded));
      } catch (Exception e) {
        LOGGER.error(e.toString());
        return site.ycsb.Status.ERROR;
      }
    }

    StructType struct = StructType.of(types);
    ListValue data = ListValue.of(struct.newValue(ydbValues));

    try {
      insertInflightLeft.decrementAndGet();

      String tablepath = this.database + "/" + tablename;
      CompletableFuture<Status> future = this.retryctx.supplyStatus(
          session -> session.executeBulkUpsert(tablepath, data, new BulkUpsertSettings()));

      if (insertInflight <= 1) {
        future.join().expect("bulk upsert problem for key");
        insertInflightLeft.incrementAndGet();
      } else {
        future.thenAccept(status -> {
            if (!status.isSuccess()) {
              LOGGER.error(String.format("Bulk upsert failed: %s", status.toString()));
            }
          }).thenRun(() -> insertInflightLeft.incrementAndGet());
        while (insertInflightLeft.get() == 0) {
          Thread.sleep(1);
        }
      }
      return site.ycsb.Status.OK;
    } catch (Exception e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status update(String table, String key, Map<String, ByteIterator> values) {
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
    String query = "DECLARE $key as Utf8; DELETE from " + table + " WHERE " + KEY_COLUMN_NAME + " = $key;";
    LOGGER.debug(query);

    Params params = Params.of("$key", PrimitiveValue.utf8(key));

    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      ExecuteDataQuerySettings executeSettings = new ExecuteDataQuerySettings().keepInQueryCache();
      StatusCode code =
          this.retryctx.supplyResult(
              session -> session.executeDataQuery(query, txControl, params, executeSettings))
              .join().getCode();
      switch (code) {
      case SUCCESS:
        return site.ycsb.Status.OK;
      case NOT_FOUND:
        return site.ycsb.Status.NOT_FOUND;
      default:
        return site.ycsb.Status.ERROR;
      }
    } catch (Exception e) {
      return site.ycsb.Status.ERROR;
    }
  }
}
