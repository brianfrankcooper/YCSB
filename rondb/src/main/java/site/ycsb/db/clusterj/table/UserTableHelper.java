/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
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
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */
package site.ycsb.db.clusterj.table;

import com.mysql.clusterj.ColumnType;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for creating DTO.
 */
public final class UserTableHelper {

  public static final String KEY = "YCSB_KEY";

  private UserTableHelper() {

  }

  public static DynamicObject createDTO(ClassGenerator classGenerator, Session session, String tableName, String keyVal,
                                        Map<String, ByteIterator> values) throws Exception {

    DynamicObject persistable = getTableObject(classGenerator, session, tableName);
    setFieldValue(persistable, KEY, keyVal.getBytes(), keyVal.getBytes().length);

    if (values != null) {
      for (String colName : values.keySet()) {
        byte[] value = values.get(colName).toArray();
        setFieldValue(persistable, colName, value, value.length);
      }
    }

    return persistable;
  }

  private static void setFieldValue(DynamicObject persistable, String colName, byte[] value, int lenght) {
    boolean found = false;
    for (int i = 0; i < persistable.columnMetadata().length; i++) {
      String fieldName = persistable.columnMetadata()[i].name();
      if (fieldName.equals(colName)) {
        int maxLength = persistable.columnMetadata()[i].maximumLength();
        if (maxLength < lenght) {
          throw new IllegalArgumentException("Column \"" + colName + "\" can only store " +
              maxLength + ". Request length: " + lenght);
        }

        ColumnType cType = persistable.columnMetadata()[i].columnType();
        if (cType == ColumnType.Varchar || cType == ColumnType.Longvarchar) {
          persistable.set(i, new String(value, StandardCharsets.UTF_8));
        } else if (cType == ColumnType.Varbinary || cType == ColumnType.Longvarbinary) {
          persistable.set(i, value);
        } else {
          throw new UnsupportedOperationException(persistable.columnMetadata()[i].columnType() +
              " is not supported in this benchmark");
        }

        found = true;
      }
    }
    if (!found) {
      throw new IllegalArgumentException("Column \"" + colName + "\" not found in the table");
    }
  }

  public static HashMap<String, ByteIterator> readFieldsFromDTO(DynamicObject dto, Set<String> fields) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for (String field : fields) {
      values.put(field, readFieldFromDTO(field, dto));
    }
    return values;
  }

  public static ByteIterator readFieldFromDTO(String colName, DynamicObject row) {
    for (int i = 0; i < row.columnMetadata().length; i++) {
      String fieldName = row.columnMetadata()[i].name();
      ColumnType cType = row.columnMetadata()[i].columnType();
      if (fieldName.equals(colName)) {

        if (cType == ColumnType.Varchar || cType == ColumnType.Longvarchar) {
          String data = (String) row.get(i);
          return new ByteArrayByteIterator(data.getBytes());
        } else if (cType == ColumnType.Varbinary || cType == ColumnType.Longvarbinary) {
          byte[] data = (byte[]) row.get(i);
          return new ByteArrayByteIterator(data, 0, data.length);
        } else {
          throw new UnsupportedOperationException(cType +
              " is not supported in this benchmark");
        }
      }
    }
    throw new IllegalArgumentException("Column \"" + colName + "\" not found in the table");
  }

  public static DynamicObject getTableObject(ClassGenerator classGenerator, Session session, String tableName)
      throws Exception {
    Class<?> tableClass = getTableClass(classGenerator, tableName);
    return (DynamicObject) session.newInstance(tableClass);
  }

  public static Class<?> getTableClass(ClassGenerator classGenerator, String tableName) throws Exception {
    return classGenerator.generateClass(tableName);
  }

  public static void setPK(String pk, DynamicObject row) {
    boolean set = false;
    for (int i = 0; i < row.columnMetadata().length; i++) {
      String fieldName = row.columnMetadata()[i].name();
      if (fieldName.equals(KEY)) {
        row.set(i, pk);
        set = true;
        break;
      }
    }

    if(!set){
      throw new UnsupportedOperationException("Failed to set primary key for read operation");
    }
  }
}
