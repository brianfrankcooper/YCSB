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
package site.ycsb.db.ignite3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

/**
 * Ignite3 key-value client with using transactions.
 */
public class IgniteTxKvClient extends IgniteClient {
  /** */
  private static final Logger LOG = LogManager.getLogger(IgniteTxKvClient.class);

  /** Transaction. */
  private Transaction tx;

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      tx = ignite.transactions().begin(txOptions);

      put(tx, key, values);

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error inserting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      tx = ignite.transactions().begin(txOptions);

      for (int i = 0; i < keys.size(); i++) {
        put(tx, keys.get(i), values.get(i));
      }

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error inserting batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error inserting batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      Status status;

      tx = ignite.transactions().begin(txOptions);

      status = get(tx, key, fields, result);

      tx.commit();

      return status;
    } catch (TransactionException txEx) {
      LOG.error("Error reading key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
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
      tx = ignite.transactions().begin(txOptions);

      for (int i = 0; i < keys.size(); i++) {
        HashMap<String, ByteIterator> result = new HashMap<>();

        Status status = get(tx, keys.get(i), fields.get(i), result);

        if (!status.isOk()) {
          throw new TransactionException(-1, String.format("Unable to read key %s", keys.get(i)));
        }

        results.add(result);
      }

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error reading batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error reading batch of keys.", e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      tx = ignite.transactions().begin(txOptions);

      kvView.remove(tx, Tuple.create(1).set(PRIMARY_COLUMN_NAME, key));

      tx.commit();

      return Status.OK;
    } catch (TransactionException txEx) {
      LOG.error("Error deleting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);

      return Status.ERROR;
    }
  }
}
