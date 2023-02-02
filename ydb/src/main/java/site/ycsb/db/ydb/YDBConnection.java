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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.Client;
import site.ycsb.DBException;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.auth.TokenAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * Thread-shared YDB connection.
 */
public class YDBConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(YDBConnection.class);

  private static final String KEY_DSN = "dsn";
  private static final String KEY_TOKEN = "token";

  private static final Map<Properties, YDBConnection> CACHE = new HashMap<>();

  public static YDBConnection openConnection(Properties props) throws DBException {
    synchronized (CACHE) {
      YDBConnection connection = CACHE.get(props);
      if (connection == null) {
        connection = createConnection(props);
        connection.addTable(new YDBTable(props));
        CACHE.put(props, connection);
      }
      connection.register();
      return connection;
    }
  }

  private final GrpcTransport transport;
  private final TableClient tableClient;
  private final SessionRetryContext retryCtx;
  private final int inflightSize;

  private final Map<String, YDBTable> tables = new HashMap<>();

  private final AtomicInteger clientCounter = new AtomicInteger(0);

  public YDBConnection(GrpcTransport transport, TableClient tableClient, int inflightSize) {
    this.transport = transport;
    this.tableClient = tableClient;
    this.retryCtx = SessionRetryContext.create(tableClient).build();
    this.inflightSize = inflightSize;
  }

  public void addTable(YDBTable table) {
    tables.put(table.name(), table);
  }

  public Collection<YDBTable> tables() {
    return tables.values();
  }

  public int inflightSize() {
    return this.inflightSize;
  }

  public void register() throws DBException {
    if (clientCounter.getAndIncrement() == 0) {
      for (YDBTable table: tables.values()) {
        table.init(this);
      }
    }
  }

  public boolean close() throws DBException {
    if (clientCounter.decrementAndGet() == 0) {
      for (YDBTable table: tables.values()) {
        table.clean(this);
      }
      tableClient.close();
      transport.close();
      return true;
    }
    return false;
  }

  public String getDatabase() {
    return transport.getDatabase();
  }

  public YDBTable findTable(String tableName) {
    YDBTable table = tables.get(tableName);
    if (table == null) {
      LOGGER.warn("find unknown table {}", tableName);
      throw new RuntimeException("Table " + tableName + " not found");
    }
    return table;
  }

  public <T> CompletableFuture<Result<T>> executeResult(Function<Session, CompletableFuture<Result<T>>> fn) {
    return retryCtx.supplyResult(fn);
  }

  public CompletableFuture<Status> executeStatus(Function<Session, CompletableFuture<Status>> fn) {
    return retryCtx.supplyStatus(fn);
  }

  private static YDBConnection createConnection(Properties props) throws DBException {
    String url = props.getProperty(KEY_DSN, null);
    if (url == null) {
      throw new DBException("ERROR: Missing data source name");
    }

    if (!url.startsWith("grpc")) {
      throw new DBException("Invalid data source name: '" + url + ";. Must be of the form 'grpc[s]://url:port'");
    }

    AuthProvider authProvider = CloudAuthHelper.getAuthProviderFromEnviron();
    String token = props.getProperty(KEY_TOKEN, null);
    if (token != null) {
      authProvider = new TokenAuthProvider(token);
    }

    LOGGER.info("Create grpc transport with dsn {}", url);
    GrpcTransport transport = GrpcTransport.forConnectionString(url)
        .withAuthProvider(authProvider)
        .build();

    try {
      int threadCount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
      int inflightSize = Integer.parseInt(props.getProperty("insertInflight", "1"));

      int maxPoolSize = threadCount * inflightSize;
      LOGGER.info("Create table client with session pool max size {}", maxPoolSize);
      TableClient tableClient = TableClient.newClient(transport)
          .sessionPoolSize(0, maxPoolSize)
          .build();

      return new YDBConnection(transport, tableClient, inflightSize);
    } catch (RuntimeException ex) {
      transport.close();
      throw ex;
    }
  }
}
