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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Execute a JDBC command line.
 * 
 * @author sudipto
 *
 */
public class JdbcDBCli implements JdbcDBClientConstants {
  
  private static void usageMessage() {
    System.out.println("JdbcCli. Options:");
    System.out.println("  -p   key=value properties defined.");
    System.out.println("  -P   location of the properties file to load.");
    System.out.println("  -c   SQL command to execute.");
  }
  
  private static void executeCommand(Properties props, String sql)
  throws SQLException {
    String driver = props.getProperty(DRIVER_CLASS);
    String username = props.getProperty(CONNECTION_USER);
    String password = props.getProperty(CONNECTION_PASSWD, "");
    String url = props.getProperty(CONNECTION_URL);
    if (driver == null || username == null || url == null) {
      throw new SQLException("Missing connection information.");
    }
    
    Connection conn = null;
    
    try {
      Class.forName(driver);
      
      conn = DriverManager.getConnection(url, username, password);
      Statement stmt = conn.createStatement();
      stmt.execute(sql);
      System.out.println("Command  \"" + sql + "\" successfully executed.");
    } catch (ClassNotFoundException e) {
      throw new SQLException("JDBC Driver class not found.");
    } finally {
      if (conn != null) {
        System.out.println("Closing database connection.");
        conn.close();
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args.length == 0) {
      usageMessage();
      System.exit(0);
    }
    
    Properties props = new Properties();
    Properties fileprops = new Properties();
    String sql = null;

    // parse arguments
    int argindex = 0;
    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println(e.getMessage());
          System.exit(0);
        }

        // Issue #5 - remove call to stringPropertyNames to make compilable
        // under Java 1.5
        for (Enumeration<?> e = myfileprops.propertyNames(); e
            .hasMoreElements();) {
          String prop = (String) e.nextElement();

          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else if (args[argindex].compareTo("-c") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        sql = args[argindex++];
      } else {
        System.out.println("Unknown option " + args[argindex]);
        usageMessage();
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      System.exit(0);
    }

    // overwrite file properties with properties from the command line

    // Issue #5 - remove call to stringPropertyNames to make compilable under
    // Java 1.5
    for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }
    
    if (sql == null) {
      System.err.println("Missing command.");
      usageMessage();
      System.exit(1);
    }
    
    try {
      executeCommand(fileprops, sql);
    } catch (SQLException e) {
      System.err.println("Error in executing command. " + e);
      System.exit(1);
    }
  }

}
