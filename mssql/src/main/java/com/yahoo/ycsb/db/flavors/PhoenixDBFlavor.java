/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.db.flavors;

import com.yahoo.ycsb.db.JdbcDBClient;
import com.yahoo.ycsb.db.StatementType;

/**
 * Database flavor for Apache Phoenix. Captures syntax differences used by Phoenix.
 */
public class PhoenixDBFlavor extends DefaultDBFlavor {
  public PhoenixDBFlavor() {
    super(DBName.PHOENIX);
  }

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    // Phoenix uses UPSERT syntax
    StringBuilder insert = new StringBuilder("UPSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + JdbcDBClient.PRIMARY_KEY + "," + insertType.getFieldString() + ")");
    insert.append(" VALUES(?");
    for (int i = 0; i < insertType.getNumFields(); i++) {
      insert.append(",?");
    }
    insert.append(")");
    return insert.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    // Phoenix doesn't have UPDATE semantics, just re-use UPSERT VALUES on the specific columns
    String[] fieldKeys = updateType.getFieldString().split(",");
    StringBuilder update = new StringBuilder("UPSERT INTO ");
    update.append(updateType.getTableName());
    update.append(" (");
    // Each column to update
    for (int i = 0; i < fieldKeys.length; i++) {
      update.append(fieldKeys[i]).append(",");
    }
    // And then set the primary key column
    update.append(JdbcDBClient.PRIMARY_KEY).append(") VALUES(");
    // Add an unbound param for each column to update
    for (int i = 0; i < fieldKeys.length; i++) {
      update.append("?, ");
    }
    // Then the primary key column's value
    update.append("?)");
    return update.toString();
  }
}
