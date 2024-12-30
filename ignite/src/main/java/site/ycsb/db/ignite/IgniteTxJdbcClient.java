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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Ignite JDBC client with using transactions.
 */
public class IgniteTxJdbcClient extends IgniteJdbcClient {
  static {
    accessMethod = "txjdbc";
  }

  /** */
  protected static final Logger LOG = LogManager.getLogger(IgniteTxJdbcClient.class);

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    super.init();

    try {
      CONN.get().setAutoCommit(false);

      int isolationLevel;

      switch (txIsolation) {
      case READ_COMMITTED:
        isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        break;
      case REPEATABLE_READ:
        isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
        break;
      case SERIALIZABLE:
      default:
        isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
      }

      CONN.get().setTransactionIsolation(isolationLevel);
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Status status = get(key, fields, result);

      CONN.get().commit();

      return status;
    } catch (SQLException e) {
      LOG.error("Error reading key in transaction. Calling rollback.", e);

      return rollback();
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
      List<Map<String, ByteIterator>> results) {
    try {
      for (int i = 0; i < keys.size(); i++) {
        HashMap<String, ByteIterator> result = new HashMap<>();

        Status status = get(keys.get(i), fields.get(i), result);

        if (!status.isOk()) {
          throw new SQLException("Error reading batch of keys.");
        }

        results.add(result);
      }

      CONN.get().commit();

      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error reading batch of keys in transaction. Calling rollback.", e);

      return rollback();
    } catch (Exception e) {
      LOG.error("Error reading batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      modify(key, values);

      CONN.get().commit();

      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error updating key in transaction. Calling rollback.", e);

      return rollback();
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

      CONN.get().commit();

      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error inserting key in transaction. Calling rollback.", e);

      return rollback();
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      for (int i = 0; i < keys.size(); i++) {
        put(keys.get(i), values.get(i));
      }

      CONN.get().commit();

      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error inserting batch of keys in transaction. Calling rollback.", e);

      return rollback();
    } catch (Exception e) {
      LOG.error("Error inserting batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      remove(key);

      CONN.get().commit();

      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error deleting key in transaction. Calling rollback.", e);

      return rollback();
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Rollback current transaction.
   */
  @NotNull
  private static Status rollback() {
    try {
      CONN.get().rollback();
    } catch (SQLException ex) {
      return Status.BAD_REQUEST;
    }

    return Status.BAD_REQUEST;
  }
}
