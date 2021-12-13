/*
 * Copyright (c) 2021, Yahoo!, Inc. All rights reserved.
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
package site.ycsb.db.table;

import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helper methods for creating DTO.
 */
public final class UserTableHelper {

  public static final String KEY = "key";

  private UserTableHelper() {

  }

  public static DynamicObject createDTO(ClassGenerator classGenerator, Session session,
                                        String tableName, String keyVal,
                                        Map<String, ByteIterator> values) throws Exception {

    DynamicObject persistable = getTableObject(classGenerator, session, tableName);
    setFieldValue(persistable, KEY, keyVal, keyVal.length());

    if (values != null) {
      for (String colName : values.keySet()) {
        byte[] value = values.get(colName).toArray();
        setFieldValue(persistable, colName, value, value.length);
      }
    }

    return persistable;
  }

  private static void setFieldValue(DynamicObject persistable, String colName, Object value,
                                    int lenght) {
    boolean found = false;
    for (int i = 0; i < persistable.columnMetadata().length; i++) {
      String fieldName = persistable.columnMetadata()[i].name();
      if (fieldName.equals(colName)) {
        int maxLength = persistable.columnMetadata()[i].maximumLength();
        if (maxLength < lenght) {
          throw new IllegalArgumentException("Column \"" + colName + "\" can only store " +
              maxLength + ". Request length: " + lenght);
        }
        persistable.set(i, value);
        found = true;
      }
    }
    if (!found) {
      throw new IllegalArgumentException("Column \"" + colName + "\" not found in the table");
    }
  }

  public static HashMap<String, ByteIterator> readFieldsFromDTO(DynamicObject dto,
                                                             Set<String> fields) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for (String field : fields) {
      values.put(field, readFieldFromDTO(field, dto));
    }
    return values;
  }

  public static ByteIterator readFieldFromDTO(String colName, DynamicObject row) {
    for (int i = 0; i < row.columnMetadata().length; i++) {
      String fieldName = row.columnMetadata()[i].name();
      if (fieldName.equals(colName)) {
        byte[] data = (byte[]) row.get(i);
        return new ByteArrayByteIterator(data, 0, data.length);
      }
    }
    throw new IllegalArgumentException("Column \"" + colName + "\" not found in the table");
  }

  static DynamicObject getTableObject(ClassGenerator classGenerator, Session session,
                                      String tableName)
      throws Exception {
    Class<?> tableClass = getTableClass(classGenerator, tableName);
    return (DynamicObject) session.newInstance(tableClass);
  }

  public static Class<?> getTableClass(ClassGenerator classGenerator,
                                       String tableName)
      throws Exception {
    return classGenerator.generateClass(tableName);
  }
}
