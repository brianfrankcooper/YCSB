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
package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.*;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.F;

import javax.cache.CacheException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ignite client.
 * <p>
 * See {@code ignite/README.md} for details.
 *
 * @author Sergey Puchnin
 * @author Taras Ledkov
 * @author Oleg Ostanin
 */
public class IgniteSqlClient extends IgniteAbstractClient {
  /** */
  private static Logger log = LogManager.getLogger(IgniteSqlClient.class);
  /** */
  private static final String PRIMARY_KEY = "YCSB_KEY";
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
    } catch (Exception e) {
      log.error(String.format("Error in processing read from table: %s", table), e);

      return Status.ERROR;
    }
  }

  /**
      Unsupported operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      return Status.OK;

    } catch (Exception e) {
      log.error(String.format("Error scanning with startkey: %s", startkey), e);
      
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
    while (true) {
      try {
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

        return Status.OK;
      } catch (CacheException e) {
        if (!e.getMessage().contains("Failed to update some keys because they had been modified concurrently")) {
          log.error(String.format("Error in processing update table: %s", table), e);

          return Status.ERROR;
        }
      } catch (Exception e) {
        log.error(String.format("Error in processing update table: %s", table), e);

        return Status.ERROR;
      }
    }
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
      InsertData insertData = new InsertData(key, values);
      StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (")
                .append(insertData.getInsertFields()).append(") VALUES (")
                .append(insertData.getInsertParams()).append(')');

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(insertData.getArgs());

      cache.query(qry).getAll();

      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error in processing insert to table: %s", table), e);

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
      StringBuilder sb = new StringBuilder("DELETE FROM ").append(table)
                .append(" WHERE ").append(PRIMARY_KEY).append(" = ?");

      SqlFieldsQuery qry = new SqlFieldsQuery(sb.toString());
      qry.setArgs(key);
      cache.query(qry).getAll();
      return Status.OK;
    } catch (Exception e) {
      log.error(String.format("Error in processing read from table: %s", table), e);

      return Status.ERROR;
    }
  }

  /**
   * Field and values for insert queries.
   */
  private static class InsertData {
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
  private static class UpdateData {
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
}
