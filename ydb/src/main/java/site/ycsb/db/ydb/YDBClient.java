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
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.StatusCode;
import com.yandex.ydb.core.UnexpectedResultException;
import com.yandex.ydb.table.SessionRetryContext;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.PrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * YDB client implementation.
 */
public class YDBClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(YDBClient.class);

  /** Key column name is 'key' (and type String). */
  private static final String KEY_COLUMN_NAME = "key";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static int fieldcount;
  private static String tablename;

  // YDB connection staff
  private String database;
  private TableClient tableclient;
  private SessionRetryContext retryctx;

  private void dropTable() throws DBException {
    Status dropstatus =
        this.retryctx.supplyStatus(session -> session.dropTable(this.database + "/" + this.tablename)).join();
    if (dropstatus.getCode() != StatusCode.SUCCESS
        && dropstatus.getCode() != StatusCode.NOT_FOUND
        && dropstatus.getCode() != StatusCode.SCHEME_ERROR) {
      String msg = "Failed to drop '" + this.tablename + "': " + dropstatus.toString();
      throw new DBException(msg);
    }
  }

  public void createTable() throws DBException {
    Properties properties = getProperties();

    final boolean doDrop = Boolean.parseBoolean(properties.getProperty("dropOnInit", "false"));
    if (doDrop) {
      dropTable();
    }

    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

    TableDescription.Builder builder = TableDescription.newBuilder();
    builder.addNullableColumn(KEY_COLUMN_NAME, PrimitiveType.utf8());
    for (int i = 0; i < fieldcount; i++) {
      builder.addNullableColumn(fieldprefix + i, PrimitiveType.utf8());
    }
    builder.setPrimaryKey(KEY_COLUMN_NAME);

    TableDescription tabledescription = builder.build();
    try {
      this.retryctx.supplyStatus(session -> session.createTable(this.database + "/" + tablename, tabledescription))
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

    fieldcount = Integer.parseInt(properties.getProperty(
      CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));

    tablename = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);

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

    GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
        .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
        .build();

    GrpcTableRpc rpc = GrpcTableRpc.ownTransport(transport);
    this.tableclient = TableClient.newClient(rpc).build();

    this.database = transport.getDatabase();
    this.retryctx = SessionRetryContext.create(this.tableclient).build();

    this.createTable();
  }

  @Override
  public void cleanup() throws DBException {
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
    String fieldsString = "*";
    if (fields != null && fields.size() > 0) {
      fieldsString = String.join(",", fields);
    }
    String query = "SELECT " + fieldsString + " FROM " + tablename + " WHERE key = '" + key + "';";

    LOGGER.debug(query);

    // Begin new transaction with SerializableRW mode
    // TODO: maybe use onlineRo()? Or at least as cmdline option?
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      // Executes data query with specified transaction control settings.
      DataQueryResult queryResult = this.retryctx.supplyResult(session -> session.executeDataQuery(query, txControl))
          .join().expect("execute read query");

      if (queryResult.getResultSetCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      ResultSetReader rs = queryResult.getResultSet(0);
      if (rs.getRowCount() == 0) {
        return site.ycsb.Status.NOT_FOUND;
      }

      while (rs.next()) {
        for (int i = 0; i < rs.getColumnCount(); ++i) {
          final byte[] val = rs.getColumn(i).getUtf8().getBytes();
          result.put(rs.getColumnName(i), new ByteArrayByteIterator(val));
        }
      }
    } catch (Exception e) {
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
    String query = "SELECT " + fieldsString + " FROM " + tablename
        + " WHERE key >= '" + startkey + "'"
        + " LIMIT " + recordcount + ";";

    LOGGER.debug(query);

    // Begin new transaction with SerializableRW mode
    // TODO: maybe use onlineRo()? Or at least as cmdline option?
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      // Executes data query with specified transaction control settings.
      DataQueryResult queryResult = this.retryctx.supplyResult(session -> session.executeDataQuery(query, txControl))
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
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  private site.ycsb.Status insertOrUpdate(String table, String key, Map<String, ByteIterator> values, String op) {
    // TODO: consider batching multiple updates like MongoDbClient does? At least control it by an cmdline option
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

    // Begin new transaction with SerializableRW mode
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      // Executes data query with specified transaction control settings.
      this.retryctx.supplyResult(session -> session.executeDataQuery(query, txControl))
        .join().expect("execute update query problem");
    } catch (Exception e) {
      LOGGER.error(e.toString());
      return site.ycsb.Status.ERROR;
    }

    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status update(String table, String key, Map<String, ByteIterator> values) {
    // note that is is a blind update: i.e. we will never return NOT_FOUND
    return insertOrUpdate(table, key, values, "UPSERT");
  }

  @Override
  public site.ycsb.Status insert(String table, String key, Map<String, ByteIterator> values) {
    // note that inserting same key twice results into error
    return insertOrUpdate(table, key, values, "INSERT");
  }

  @Override
  public site.ycsb.Status delete(String table, String key) {
    String query = "DELETE from " + table + " WHERE " + KEY_COLUMN_NAME + " = '" + key + "'";
    LOGGER.debug(query);

    // Begin new transaction with SerializableRW mode
    TxControl txControl = TxControl.serializableRw().setCommitTx(true);

    try {
      // Executes data query with specified transaction control settings.
      StatusCode code =
          this.retryctx.supplyResult(session -> session.executeDataQuery(query, txControl)).join().getCode();
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
