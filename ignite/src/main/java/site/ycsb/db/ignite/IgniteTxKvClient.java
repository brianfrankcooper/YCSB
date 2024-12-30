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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

/**
 * Ignite key-value client with using transactions.
 */
public class IgniteTxKvClient extends IgniteClient {
  static {
    accessMethod = "txkv";
  }

  /** */
  protected static final Logger LOG = LogManager.getLogger(IgniteTxKvClient.class);

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      txStart();

      BinaryObject binObj = cache.get(key);

      tx.commit();

      return convert(binObj, fields, result);
    } catch (IgniteException txEx) {
      LOG.error("Error reading key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchRead(String table, List<String> keys, List<Set<String>> fields,
                          List<Map<String, ByteIterator>> results) {
    try {
      txStart();

      for (int i = 0; i < keys.size(); i++) {
        BinaryObject binObj = cache.get(keys.get(i));

        Map<String, ByteIterator> record = new HashMap<>();

        Status status = convert(binObj, fields.get(i), record);

        if (!status.isOk()) {
          throw new IgniteException("Error reading batch of keys.");
        }

        results.add(record);
      }

      tx.commit();

      return Status.OK;
    } catch (IgniteException txEx) {
      LOG.error("Error reading batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error reading batch of keys.", e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      txStart();

      cache.invoke(key, new IgniteClient.Updater(values));

      tx.commit();

      return Status.OK;
    } catch (IgniteException txEx) {
      LOG.error("Error updating key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error updating key: %s", key), e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      txStart();

      BinaryObject binObj = convert(values);

      cache.put(key, binObj);

      tx.commit();

      return Status.OK;
    } catch (IgniteException txEx) {
      LOG.error("Error inserting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status batchInsert(String table, List<String> keys, List<Map<String, ByteIterator>> values) {
    try {
      txStart();

      for (int i = 0; i < keys.size(); i++) {
        BinaryObject binObj = convert(values.get(i));

        cache.put(keys.get(i), binObj);
      }

      tx.commit();

      return Status.OK;
    } catch (IgniteException txEx) {
      LOG.error("Error inserting batch of keys in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error("Error inserting batch of keys.", e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    try {
      txStart();

      cache.remove(key);

      tx.commit();

      return Status.OK;
    } catch (IgniteException txEx) {
      LOG.error("Error deleting key in transaction. Calling rollback.", txEx);
      tx.rollback();

      throw txEx;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);

      return Status.ERROR;
    } finally {
      tx.close();
    }
  }
}
