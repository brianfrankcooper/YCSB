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
package site.ycsb.db.flavors;

import site.ycsb.db.JdbcDBClient;
import site.ycsb.db.StatementType;

/**
 * Database flavor for CockroachDB. Captures syntax differences used by CockroachDB as of system time.
 */
public class CockroachDBFlavor extends DefaultDBFlavor {
  private String aost;

  public CockroachDBFlavor() {
    super(DBName.COCKROACH);
    this.aost="'-5s'";
  }
  public CockroachDBFlavor(String aost) {
    super(DBName.COCKROACH);    
    this.aost = aost;
  }

  @Override
  public String createReadStatement(StatementType readType, String key) {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.getTableName());
    read.append(" AS OF SYSTEM TIME " + this.aost);
    read.append(" WHERE ");
    read.append(JdbcDBClient.PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    System.err.println("CockroachDB: " + read.toString());
    return read.toString();
  }

  @Override
  public  String createScanStatement(StatementType scanType, String key,
                                     boolean sqlserverScans, boolean sqlansiScans) {
    StringBuilder select = new StringBuilder("SELECT * FROM ");
    select.append(scanType.getTableName());
    select.append(" AS OF SYSTEM TIME " + this.aost);
    select.append(" WHERE ");
    select.append(JdbcDBClient.PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(JdbcDBClient.PRIMARY_KEY);
    select.append(" LIMIT ?");
    System.err.println("CockroachDB: " + select.toString());
    return select.toString();
  }
}
