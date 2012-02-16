/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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
package com.yahoo.ycsb.db;

/**
 * Constants used by the JDBC client.
 * 
 * @author sudipto
 *
 */
public interface JdbcDBClientConstants {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";
  
  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";
  
  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";
  
  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";
  
  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY="fieldcount";
  
  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";
  
  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";
  
  /** The code to return when the call succeeds. */
  public static final int SUCCESS = 0;
  
  /** The primary key in the user table.*/
  public static String PRIMARY_KEY = "YCSB_KEY";
  
  /** The field name prefix in the table.*/
  public static String COLUMN_PREFIX = "FIELD";
}
