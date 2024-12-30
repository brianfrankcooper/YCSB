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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Ignite client.
 * <p>
 * See {@code ignite/README.md} for details.
 */
public class IgniteSqlClient extends IgniteAbstractClient {
  static {
    accessMethod = "sql";
  }

  /** */
  protected static final Logger LOG = LogManager.getLogger(IgniteSqlClient.class);

  /** */
  protected static final String PRIMARY_KEY = "YCSB_KEY";

  /** {@inheritDoc} */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      return get(table, key, fields, result);
    } catch (Exception e) {
      LOG.error(String.format("Error reading key: %s", key), e);

      return Status.ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    while (true) {
      try {
        modify(table, key, values);

        return Status.OK;
      } catch (CacheException e) {
        if (!e.getMessage().contains("Failed to update some keys because they had been modified concurrently")) {
          LOG.error(String.format("Error in processing update table: %s", table), e);

          return Status.ERROR;
        }
      } catch (Exception e) {
        LOG.error(String.format("Error updating key: %s", key), e);

        return Status.ERROR;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      put(table, key, values);

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
      remove(table, key);

      return Status.OK;
    } catch (Exception e) {
      LOG.error(String.format("Error deleting key: %s", key), e);

      return Status.ERROR;
    }
  }

  /**
   * Field and values for insert queries.
   */
  protected static class InsertData {
    private final Object[] args;
    private final String insertFields;
    private final String insertParams;

    /**
     * @param key    Key.
     * @param values Field values.
     */
    InsertData(String key, Map<String, ByteIterator> values) {
      args = new String[values.size() + 1];

      int idx = 0;
      args[idx++] = key;

      StringBuilder sbFields = new StringBuilder(PRIMARY_KEY);
      StringBuilder sbParams = new StringBuilder("?");

      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        args[idx++] = e.getValue().toString();
        sbFields.append(',').append(e.getKey());
        sbParams.append(", ?");
      }

      insertFields = sbFields.toString();
      insertParams = sbParams.toString();
    }

    public Object[] getArgs() {
      return args;
    }

    public String getInsertFields() {
      return insertFields;
    }

    public String getInsertParams() {
      return insertParams;
    }
  }

  /**
   * Field and values for update queries.
   */
  protected static class UpdateData {
    private final Object[] args;
    private final String[] fields;

    /**
     * @param key    Key.
     * @param values Field values.
     */
    UpdateData(String key, Map<String, ByteIterator> values) {
      args = new String[values.size() + 1];
      fields = new String[values.size()];

      int idx = 0;

      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        args[idx] = e.getValue().toString();
        fields[idx++] = e.getKey();
      }

      args[idx] = key;
    }

    public Object[] getArgs() {
      return args;
    }

    public String[] getFields() {
      return fields;
    }
  }

  /**
   * Perform single INSERT operation with Ignite SQL.
   *
   * @param table Table.
   * @param key Key.
   * @param values Values.
   */
  protected void put(String table, String key, Map<String, ByteIterator> values) {
    InsertData insertData = new InsertData(key, values);
    StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (")
        .append(insertData.getInsertFields()).append(") VALUES (")
        .append(insertData.getInsertParams()).append(')');

    SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
    qry.setArgs(insertData.getArgs());

    cache.query(qry).getAll();
  }

  /**
   * Perform single SELECT operation with Ignite SQL.
   *
   * @param table Table.
   * @param key Key.
   * @param fields Fields.
   * @param result Result.
   */
  @NotNull
  protected Status get(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    StringBuilder sb = new StringBuilder("SELECT * FROM ").append(table)
        .append(" WHERE ").append(PRIMARY_KEY).append("=?");

    SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
    qry.setArgs(key);

    FieldsQueryCursor<List<?>> cur = cache.query(qry);
    Iterator<List<?>> it = cur.iterator();

    if (!it.hasNext()) {
      return Status.NOT_FOUND;
    }

    String[] colNames = new String[cur.getColumnsCount()];
    for (int i = 0; i < colNames.length; ++i) {
      String colName = cur.getFieldName(i);
      if (F.isEmpty(fields)) {
        colNames[i] = colName.toLowerCase();
      } else {
        for (String f : fields) {
          if (f.equalsIgnoreCase(colName)) {
            colNames[i] = f;
          }
        }
      }
    }

    while (it.hasNext()) {
      List<?> row = it.next();

      for (int i = 0; i < colNames.length; ++i) {
        if (colNames[i] != null) {
          result.put(colNames[i], new StringByteIterator((String) row.get(i)));
        }
      }
    }

    return Status.OK;
  }

  /**
   * Perform single UPDATE operation with Ignite SQL.
   *
   * @param table Table.
   * @param key Key.
   * @param values Values.
   */
  protected void modify(String table, String key, Map<String, ByteIterator> values) {
    UpdateData updData = new UpdateData(key, values);
    StringBuilder sb = new StringBuilder("UPDATE ").append(table).append(" SET ");

    for (int i = 0; i < updData.getFields().length; ++i) {
      sb.append(updData.getFields()[i]).append("=?");
      if (i < updData.getFields().length - 1) {
        sb.append(", ");
      }
    }

    sb.append(" WHERE ").append(PRIMARY_KEY).append("=?");

    SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
    qry.setArgs(updData.getArgs());

    cache.query(qry).getAll();
  }

  /**
   * Perform single DELETE operation with Ignite SQL.
   *
   * @param table Table.
   * @param key Key.
   */
  protected void remove(String table, String key) {
    StringBuilder sb = new StringBuilder("DELETE FROM ").append(table)
        .append(" WHERE ").append(PRIMARY_KEY).append(" = ?");

    SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
    qry.setArgs(key);
    cache.query(qry).getAll();
  }
}
