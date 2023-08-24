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

import java.util.HashSet;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Ignite3 key-value client.
 */
public class IgniteClient extends IgniteAbstractClient {
  /**
   *
   */
  private static final Logger LOG = LogManager.getLogger(IgniteClient.class);

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      Tuple tKey = Tuple.create(1).set(PRIMARY_COLUMN_NAME, key);
      Tuple tValues = kvView.get(null, tKey);

      if (tValues == null) {
        return Status.NOT_FOUND;
      }

      if (fields == null || fields.isEmpty()) {
        fields = new HashSet<>();
        for (int iter = 0; iter < tValues.columnCount(); iter++) {
          fields.add(tValues.columnName(iter));
        }
      }

      for (String column : fields) {
        if (!Objects.equals(tValues.stringValue(column), null)) {
          result.put(column, new StringByteIterator(tValues.stringValue(column)));
        }
      }

      if (debug) {
        LOG.info("table:{" + table + "}, key:{" + key + "}" + ", fields:{" + fields + "}");
        LOG.info("result {" + result + "}");
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Tuple value = Tuple.create(fieldCount);

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if (debug) {
          LOG.info("key:" + key + "; " + entry.getKey() + "!!!" + entry.getValue());
        }
        value.set(entry.getKey(), entry.getValue().toString());
      }

      if (table.equals(cacheName)) {
        kvView.put(null, Tuple.create(1).set(PRIMARY_COLUMN_NAME, key), value);
      } else {
        throw new UnsupportedOperationException("Unexpected table name: " + table);
      }

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error inserting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    try {
      kvView.remove(null, Tuple.create(1).set(PRIMARY_COLUMN_NAME, key));

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s ", key), e);
    }

    return Status.ERROR;
  }
}
