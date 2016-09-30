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

import com.yahoo.ycsb.db.StatementType;

/**
 * DBFlavor captures minor differences in syntax and behavior among JDBC implementations and SQL
 * dialects. This class also acts as a factory to instantiate concrete flavors based on the JDBC URL.
 */
public abstract class DBFlavor {

  enum DBName {
    DEFAULT,
    PHOENIX
  }

  private final DBName dbName;

  public DBFlavor(DBName dbName) {
    this.dbName = dbName;
  }

  public static DBFlavor fromJdbcUrl(String url) {
    if (url.startsWith("jdbc:phoenix")) {
      return new PhoenixDBFlavor();
    }
    return new DefaultDBFlavor();
  }

  /**
   * Create and return a SQL statement for inserting data.
   */
  public abstract String createInsertStatement(StatementType insertType, String key);

  /**
   * Create and return a SQL statement for reading data.
   */
  public abstract String createReadStatement(StatementType readType, String key);

  /**
   * Create and return a SQL statement for deleting data.
   */
  public abstract String createDeleteStatement(StatementType deleteType, String key);

  /**
   * Create and return a SQL statement for updating data.
   */
  public abstract String createUpdateStatement(StatementType updateType, String key);

  /**
   * Create and return a SQL statement for scanning data.
   */
  public abstract String createScanStatement(StatementType scanType, String key);
}
