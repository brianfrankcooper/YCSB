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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import static org.apache.ignite.table.criteria.Criteria.columnValue;
import static org.apache.ignite.table.criteria.Criteria.equalTo;

/**
 * Ignite3 criteria query client.
 */
public class IgniteCriteriaClient extends IgniteAbstractClient {
  /** Logger. */
  private static final Logger LOG = LogManager.getLogger(IgniteCriteriaClient.class);

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try (Cursor<Entry<Tuple, Tuple>> cursor = kvView.query(null, columnValue(PRIMARY_COLUMN_NAME, equalTo(key)))) {
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }

      Tuple tValues = cursor.next().getValue();

      if (fields == null || fields.isEmpty()) {
        int sz = tValues.columnCount();

        fields = new HashSet<>(IgniteUtils.capacity(sz));

        for (int iter = 0; iter < sz; iter++) {
          fields.add(tValues.columnName(iter));
        }
      }

      for (String column : fields) {
        String colVal = tValues.stringValue(column);

        if (colVal != null) {
          result.put(column, new StringByteIterator(colVal));
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

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  /** {@inheritDoc} */
  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
