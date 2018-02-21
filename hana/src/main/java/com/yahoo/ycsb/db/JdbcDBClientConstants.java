/*
 * Copyright (c) 2015 - 2018 Anita Armbruster, Andreas Bader, Patrick Dabbert, Pascal Litty,
 *               2018 YCSB Contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

/**
 * Constants used by the JDBC client.
 *
 * @author sudipto
 */
public final class JdbcDBClientConstants {

  // shut checkstyle up
  private JdbcDBClientConstants() {}

  /**
   * The class to use as the jdbc driver.
   */
  public static final String DRIVER_CLASS = "db.driver";

  /**
   * The URL to connect to the database.
   */
  public static final String CONNECTION_URL = "db.url";

  /**
   * The user name to use to connect to the database.
   */
  public static final String CONNECTION_USER = "db.user";

  /**
   * The password to use for establishing the connection.
   */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /**
   * The JDBC fetch size hinted to the driver.
   */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /**
   * The JDBC connection auto-commit property for the driver.
   */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /**
   * Representing a NULL value.
   */
  public static final String NULL_VALUE = "NULL";

  /**
   * The Timestamp key in the user table.
   */
  public static final String TIMESTAMP_KEY = "YCSB_KEY";
}
