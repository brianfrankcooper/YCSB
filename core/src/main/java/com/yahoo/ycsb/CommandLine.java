/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import com.yahoo.ycsb.workloads.CoreWorkload;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * A simple command line client to a database, using the appropriate com.yahoo.ycsb.DB implementation.
 */
public final class CommandLine {
  private CommandLine() {
    //not used
  }

  public static final String DEFAULT_DB = "com.yahoo.ycsb.BasicDB";

  public static void usageMessage() {
    System.out.println("YCSB Command Line Client");
    System.out.println("Usage: java com.yahoo.ycsb.CommandLine [options]");
    System.out.println("Options:");
    System.out.println("  -P filename: Specify a property file");
    System.out.println("  -p name=value: Specify a property value");
    System.out.println("  -db classname: Use a specified DB class (can also set the \"db\" property)");
    System.out.println("  -table tablename: Use the table name instead of the default \"" +
        CoreWorkload.TABLENAME_PROPERTY_DEFAULT + "\"");
    System.out.println();
  }

  public static void help() {
    System.out.println("Commands:");
    System.out.println("  read key [field1 field2 ...] - Read a record");
    System.out.println("  scan key recordcount [field1 field2 ...] - Scan starting at key");
    System.out.println("  insert key name1=value1 [name2=value2 ...] - Insert a new record");
    System.out.println("  update key name1=value1 [name2=value2 ...] - Update a record");
    System.out.println("  delete key - Delete a record");
    System.out.println("  table [tablename] - Get or [set] the name of the table");
    System.out.println("  quit - Quit");
  }

  public static void main(String[] args) {

    Properties props = new Properties();
    Properties fileprops = new Properties();

    parseArguments(args, props, fileprops);

    for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;

    System.out.println("YCSB Command Line client");
    System.out.println("Type \"help\" for command line help");
    System.out.println("Start with \"-help\" for usage info");

    String table = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);

    //create a DB
    String dbname = props.getProperty(Client.DB_PROPERTY, DEFAULT_DB);

    ClassLoader classLoader = CommandLine.class.getClassLoader();

    DB db = null;

    try {
      Class dbclass = classLoader.loadClass(dbname);
      db = (DB) dbclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }

    db.setProperties(props);
    try {
      db.init();
    } catch (DBException e) {
      e.printStackTrace();
      System.exit(0);
    }

    System.out.println("Connected.");

    //main loop
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    for (;;) {
      //get user input
      System.out.print("> ");

      String input = null;

      try {
        input = br.readLine();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }

      if (input.compareTo("") == 0) {
        continue;
      }

      if (input.compareTo("help") == 0) {
        help();
        continue;
      }

      if (input.compareTo("quit") == 0) {
        break;
      }

      String[] tokens = input.split(" ");

      long st = System.currentTimeMillis();
      //handle commands
      if (tokens[0].compareTo("table") == 0) {
        handleTable(tokens, table);
      } else if (tokens[0].compareTo("read") == 0) {
        handleRead(tokens, table, db);
      } else if (tokens[0].compareTo("scan") == 0) {
        handleScan(tokens, table, db);
      } else if (tokens[0].compareTo("update") == 0) {
        handleUpdate(tokens, table, db);
      } else if (tokens[0].compareTo("insert") == 0) {
        handleInsert(tokens, table, db);
      } else if (tokens[0].compareTo("delete") == 0) {
        handleDelete(tokens, table, db);
      } else {
        System.out.println("Error: unknown command \"" + tokens[0] + "\"");
      }

      System.out.println((System.currentTimeMillis() - st) + " ms");
    }
  }

  private static void parseArguments(String[] args, Properties props, Properties fileprops) {
    int argindex = 0;
    while ((argindex < args.length) && (args[argindex].startsWith("-"))) {
      if ((args[argindex].compareTo("-help") == 0) ||
          (args[argindex].compareTo("--help") == 0) ||
          (args[argindex].compareTo("-?") == 0) ||
          (args[argindex].compareTo("--?") == 0)) {
        usageMessage();
        System.exit(0);
      }

      if (args[argindex].compareTo("-db") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        props.setProperty(Client.DB_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-P") == 0) {
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

        for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
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
      } else if (args[argindex].compareTo("-table") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        props.put(CoreWorkload.TABLENAME_PROPERTY, args[argindex]);

        argindex++;
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
  }

  private static void handleDelete(String[] tokens, String table, DB db) {
    if (tokens.length != 2) {
      System.out.println("Error: syntax is \"delete keyname\"");
    } else {
      Status ret = db.delete(table, tokens[1]);
      System.out.println("Return result: " + ret.getName());
    }
  }

  private static void handleInsert(String[] tokens, String table, DB db) {
    if (tokens.length < 3) {
      System.out.println("Error: syntax is \"insert keyname name1=value1 [name2=value2 ...]\"");
    } else {
      HashMap<String, ByteIterator> values = new HashMap<>();

      for (int i = 2; i < tokens.length; i++) {
        String[] nv = tokens[i].split("=");
        values.put(nv[0], new StringByteIterator(nv[1]));
      }

      Status ret = db.insert(table, tokens[1], values);
      System.out.println("Result: " + ret.getName());
    }
  }

  private static void handleUpdate(String[] tokens, String table, DB db) {
    if (tokens.length < 3) {
      System.out.println("Error: syntax is \"update keyname name1=value1 [name2=value2 ...]\"");
    } else {
      HashMap<String, ByteIterator> values = new HashMap<>();

      for (int i = 2; i < tokens.length; i++) {
        String[] nv = tokens[i].split("=");
        values.put(nv[0], new StringByteIterator(nv[1]));
      }

      Status ret = db.update(table, tokens[1], values);
      System.out.println("Result: " + ret.getName());
    }
  }

  private static void handleScan(String[] tokens, String table, DB db) {
    if (tokens.length < 3) {
      System.out.println("Error: syntax is \"scan keyname scanlength [field1 field2 ...]\"");
    } else {
      Set<String> fields = null;

      if (tokens.length > 3) {
        fields = new HashSet<>();

        fields.addAll(Arrays.asList(tokens).subList(3, tokens.length));
      }

      Vector<HashMap<String, ByteIterator>> results = new Vector<>();
      Status ret = db.scan(table, tokens[1], Integer.parseInt(tokens[2]), fields, results);
      System.out.println("Result: " + ret.getName());
      int record = 0;
      if (results.isEmpty()) {
        System.out.println("0 records");
      } else {
        System.out.println("--------------------------------");
      }
      for (Map<String, ByteIterator> result : results) {
        System.out.println("Record " + (record++));
        for (Map.Entry<String, ByteIterator> ent : result.entrySet()) {
          System.out.println(ent.getKey() + "=" + ent.getValue());
        }
        System.out.println("--------------------------------");
      }
    }
  }

  private static void handleRead(String[] tokens, String table, DB db) {
    if (tokens.length == 1) {
      System.out.println("Error: syntax is \"read keyname [field1 field2 ...]\"");
    } else {
      Set<String> fields = null;

      if (tokens.length > 2) {
        fields = new HashSet<>();

        fields.addAll(Arrays.asList(tokens).subList(2, tokens.length));
      }

      HashMap<String, ByteIterator> result = new HashMap<>();
      Status ret = db.read(table, tokens[1], fields, result);
      System.out.println("Return code: " + ret.getName());
      for (Map.Entry<String, ByteIterator> ent : result.entrySet()) {
        System.out.println(ent.getKey() + "=" + ent.getValue());
      }
    }
  }

  private static void handleTable(String[] tokens, String table) {
    if (tokens.length == 1) {
      System.out.println("Using table \"" + table + "\"");
    } else if (tokens.length == 2) {
      table = tokens[1];
      System.out.println("Using table \"" + table + "\"");
    } else {
      System.out.println("Error: syntax is \"table tablename\"");
    }
  }


}
