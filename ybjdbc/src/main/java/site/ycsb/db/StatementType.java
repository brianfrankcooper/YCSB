/**
 * Copyright (c) 2010 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
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
package site.ycsb.db;

/**
 * The statement type for the prepared statements.
 */
public class StatementType {

  enum Type {
    INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);

    private final int internalType;

    private Type(int type) {
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
  private int shardIndex;
  private int numFields;
  private String tableName;
  private String fieldString;

  public StatementType(Type type, String tableName, int numFields, String fieldString, int shardIndex) {
    this.type = type;
    this.tableName = tableName;
    this.numFields = numFields;
    this.fieldString = fieldString;
    this.shardIndex = shardIndex;
  }

  public String getTableName() {
    return tableName;
  }

  public String getFieldString() {
    return fieldString;
  }

  public int getNumFields() {
    return numFields;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + numFields + 100 * shardIndex;
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    result = prime * result + ((type == null) ? 0 : type.getHashCode());
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
    if (numFields != other.numFields) {
      return false;
    }
    if (shardIndex != other.shardIndex) {
      return false;
    }
    if (tableName == null) {
      if (other.tableName != null) {
        return false;
      }
    } else if (!tableName.equals(other.tableName)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    if (!fieldString.equals(other.fieldString)) {
      return false;
    }
    return true;
  }
}
