/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */
package site.ycsb.db.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite JDBC client.
 */
public class IgniteJdbcClient extends IgniteAbstractClient {
  static {
    accessMethod = "jdbc";
  }

  /** */
  protected static final Logger LOG = LogManager.getLogger(IgniteJdbcClient.class);

  /** SQL string of prepared statement for reading values. */
  protected static String readPreparedStatementString;

  /** SQL string of prepared statement for inserting values. */
  protected static String insertPreparedStatementString;

  /** SQL string of prepared statement for deleting values. */
  protected static String deletePreparedStatementString;

  /**
   * Use separate connection per thread since sharing a single Connection object is not recommended.
   */
  protected static final ThreadLocal<Connection> CONN = ThreadLocal
      .withInitial(IgniteJdbcClient::buildConnection);

  /** Prepared statement for reading values. */
  protected static final ThreadLocal<PreparedStatement> READ_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildReadStatement);

  /** Prepared statement for inserting values. */
  protected static final ThreadLocal<PreparedStatement> INSERT_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildInsertStatement);

  /** Prepared statement for deleting values. */
  protected static final ThreadLocal<PreparedStatement> DELETE_PREPARED_STATEMENT = ThreadLocal
      .withInitial(IgniteJdbcClient::buildDeleteStatement);

  /** Build prepared statement for reading values. */
  protected static PreparedStatement buildReadStatement() {
    try {
      return CONN.get().prepareStatement(readPreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + readPreparedStatementString, e);
    }
  }

  /** Build JDBC connection. */
  protected static Connection buildConnection() {
    String hostsStr;

    if (useEmbeddedIgnite) {
      Set<String> addrs = new HashSet<>();
      ignite.cluster().nodes().forEach(clusterNode -> addrs.addAll(clusterNode.addresses()));
      hostsStr = String.join(",", addrs);
    } else {
      hostsStr = hosts;
    }

    String url = "jdbc:ignite:thin://" + hostsStr;
    try {
      return DriverManager.getConnection(url);
    } catch (Exception e) {
      throw new RuntimeException("Unable to establish connection with " + url, e);
    }
  }

  /** Build prepared statement for inserting values. */
  protected static PreparedStatement buildInsertStatement() {
    try {
      return CONN.get().prepareStatement(insertPreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + insertPreparedStatementString, e);
    }
  }

  /** Build prepared statement for deleting values. */
  protected static PreparedStatement buildDeleteStatement() {
    try {
      return CONN.get().prepareStatement(deletePreparedStatementString);
    } catch (SQLException e) {
      throw new RuntimeException("Unable to prepare statement for SQL: " + deletePreparedStatementString, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    synchronized (IgniteJdbcClient.class) {
      if (readPreparedStatementString != null || insertPreparedStatementString != null
          || deletePreparedStatementString != null) {
        return;
      }

      readPreparedStatementString = String.format("SELECT * FROM %s WHERE %s = ?", cacheName, PRIMARY_COLUMN_NAME);

      List<String> columns = new ArrayList<>(Collections.singletonList(PRIMARY_COLUMN_NAME));
      columns.addAll(FIELDS);

      String columnsString = String.join(", ", columns);

      String valuesString = String.join(", ", Collections.nCopies(columns.size(), "?"));

      insertPreparedStatementString = String.format("INSERT INTO %s (%s) VALUES (%s)",
          cacheName, columnsString, valuesString);

      deletePreparedStatementString = String.format("DELETE * FROM %s WHERE %s = ?", cacheName, PRIMARY_COLUMN_NAME);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      return get(key, fields, result);
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      modify(key, values);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error updating key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      put(key, values);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      remove(key);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    Connection conn0 = CONN.get();
    try {
      if (conn0 != null && !conn0.isClosed()) {
        if (!READ_PREPARED_STATEMENT.get().isClosed()) {
          READ_PREPARED_STATEMENT.get().close();
        }
        if (!INSERT_PREPARED_STATEMENT.get().isClosed()) {
          INSERT_PREPARED_STATEMENT.get().close();
        }
        if (!DELETE_PREPARED_STATEMENT.get().isClosed()) {
          DELETE_PREPARED_STATEMENT.get().close();
        }

        conn0.close();
        CONN.remove();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    super.cleanup();
  }

  /**
   * Set values for the prepared statement object.
   *
   * @param statement Prepared statement object.
   * @param key Key field value.
   * @param values Values.
   */
  static void setStatementValues(PreparedStatement statement, String key, Map<String, ByteIterator> values)
      throws SQLException {
    int i = 1;

    statement.setString(i++, key);

    for (String fieldName : FIELDS) {
      statement.setString(i++, values.get(fieldName).toString());
    }
  }

  /**
   * Perform single INSERT operation with Ignite SQL.
   *
   * @param key Key.
   * @param values Values.
   */
  protected void put(String key, Map<String, ByteIterator> values) throws SQLException {
    PreparedStatement stmt = INSERT_PREPARED_STATEMENT.get();

    setStatementValues(stmt, key, values);

    stmt.executeUpdate();
  }

  /**
   * Perform single SELECT operation with Ignite SQL.
   *
   * @param key Key.
   * @param fields Fields.
   * @param result Result.
   */
  @NotNull
  protected Status get(String key, Set<String> fields, Map<String, ByteIterator> result) throws SQLException {
    PreparedStatement stmt = READ_PREPARED_STATEMENT.get();

    stmt.setString(1, key);

    try (ResultSet rs = stmt.executeQuery()) {
      if (!rs.next()) {
        return Status.NOT_FOUND;
      }

      if (fields == null || fields.isEmpty()) {
        fields = new HashSet<>();
        fields.addAll(FIELDS);
      }

      for (String column : fields) {
        //+2 because indexes start from 1 and 1st one is key field
        String val = rs.getString(FIELDS.indexOf(column) + 2);

        if (val != null) {
          result.put(column, new StringByteIterator(val));
        }
      }
    }

    return Status.OK;
  }

  /**
   * Perform single UPDATE operation with Ignite SQL.
   *
   * @param key Key.
   * @param values Values.
   */
  protected void modify(String key, Map<String, ByteIterator> values) throws SQLException {
    try (Statement stmt = CONN.get().createStatement()) {

      List<String> updateValuesList = new ArrayList<>();
      for (Entry<String, ByteIterator> entry : values.entrySet()) {
        updateValuesList.add(String.format("%s='%s'", entry.getKey(), entry.getValue().toString()));
      }

      String sql = String.format("UPDATE %s SET %s WHERE %s = '%s'",
          cacheName, String.join(", ", updateValuesList), PRIMARY_COLUMN_NAME, key);

      stmt.executeUpdate(sql);
    }
  }

  /**
   * Perform single DELETE operation with Ignite SQL.
   *
   * @param key Key.
   */
  protected void remove(String key) throws SQLException {
    PreparedStatement stmt = DELETE_PREPARED_STATEMENT.get();

    stmt.setString(1, key);

    stmt.executeUpdate();
  }
}
