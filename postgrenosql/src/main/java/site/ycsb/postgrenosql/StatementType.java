/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding StatementType class.
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
package site.ycsb.postgrenosql;

import java.util.Set;

/**
 * The statement type for the prepared statements.
 */
public class StatementType {

  enum Type {
    INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);

    private final int internalType;

    Type(int type) {
      internalType = type;
    }

    int getHashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + internalType;
      return result;
    }
  }

  private Type type;
  private String tableName;
  private Set<String> fields;

  public StatementType(Type type, String tableName, Set<String> fields) {
    this.type = type;
    this.tableName = tableName;
    this.fields = fields;
  }

  public String getTableName() {
    return tableName;
  }

  public Set<String> getFields() {
    return fields;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((type == null) ? 0 : type.getHashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    StatementType other = (StatementType) obj;
    if (type != other.type) {
      return false;
    }

    if (tableName == null) {
      if (other.tableName != null) {
        return false;
      }
    } else if (!tableName.equals(other.tableName)) {
      return false;
    }

    if (fields == null) {
      if (other.fields != null) {
        return false;
      }
    }else if (!fields.equals(other.fields)) {
      return false;
    }
    return true;
  }
}
