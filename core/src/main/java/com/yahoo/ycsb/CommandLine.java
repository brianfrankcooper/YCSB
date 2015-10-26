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

package com.yahoo.ycsb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * A simple command line client to a database, using the appropriate
 * com.yahoo.ycsb.DB implementation.
 */
public final class CommandLine {
  public static final String DEFAULT_DB = "com.yahoo.ycsb.BasicDB";

  public static void help() {
    System.out.println("Commands:");
    System.out.println("  read key [field1 field2 ...] - Read a record");
    System.out.println(
        "  scan key recordcount [field1 field2 ...] - Scan starting at key");
    System.out.println(
        "  insert key name1=value1 [name2=value2 ...] - Insert a new record");
    System.out.println(
        "  update key name1=value1 [name2=value2 ...] - Update a record");
    System.out.println("  delete key - Delete a record");
    System.out
        .println("  table [tablename] - Get or [set] the name of the table");
    System.out.println("  quit - Quit");
  }

  public static void main(final String[] args) {
    int argindex = 0;

    Properties props = new Properties();
    final Properties fileprops = new Properties();
    String table = CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

    while ((argindex < args.length) && (args[argindex].startsWith("-"))) {
      if ((args[argindex].compareTo("-help") == 0)
          || (args[argindex].compareTo("--help") == 0)
          || (args[argindex].compareTo("-?") == 0)
          || (args[argindex].compareTo("--?") == 0)) {
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
        argindex = loadFile(args, argindex, fileprops);
      } else if (args[argindex].compareTo("-p") == 0) {
        argindex = setProp(args, argindex, props);
      } else if (args[argindex].compareTo("-table") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          System.exit(0);
        }
        table = args[argindex];
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

    for (final Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      final String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;

    System.out.println("YCSB Command Line client");
    System.out.println("Type \"help\" for command line help");
    System.out.println("Start with \"-help\" for usage info");

    // create a DB
    final String dbname = props.getProperty(Client.DB_PROPERTY, DEFAULT_DB);
    final ClassLoader classLoader = CommandLine.class.getClassLoader();
    DB db = null;
    try {
      final Class dbclass = classLoader.loadClass(dbname);
      db = (DB) dbclass.newInstance();
    } catch (final Exception e) {
      e.printStackTrace();
      System.exit(0);
    }

    db.setProperties(props);
    try {
      db.init();
    } catch (final DBException e) {
      e.printStackTrace();
      System.exit(0);
    }

    System.out.println("Connected.");

    // main loop
    mainLoop(table, db);
  }

  public static void usageMessage() {
    System.out.println("YCSB Command Line Client");
    System.out.println("Usage: java com.yahoo.ycsb.CommandLine [options]");
    System.out.println("Options:");
    System.out.println("  -P filename: Specify a property file");
    System.out.println("  -p name=value: Specify a property value");
    System.out
        .println("  -db classname: Use a specified DB class (can also set the "
            + "\"db\" property)");
    System.out.println(
        "  -table tablename: Use the table name instead of the default \""
            + CoreWorkload.TABLENAME_PROPERTY_DEFAULT + "\"");
    System.out.println();
  }

  /**
   * Loads properties from a file.
   * 
   * @param args
   *          The command args.
   * @param argindex
   *          The current argument index.
   * @param fileprops
   *          The property object to load.
   * @return The new argument offset.
   */
  private static int loadFile(final String[] args, int argindex,
      final Properties fileprops) {
    int index = argindex + 1;
    if (index >= args.length) {
      usageMessage();
      System.exit(0);
    }
    final String propfile = args[index];

    final Properties myfileprops = new Properties();
    try {
      myfileprops.load(new FileInputStream(propfile));
    } catch (final IOException e) {
      System.out.println(e.getMessage());
      System.exit(0);
    }

    for (final Enumeration e = myfileprops.propertyNames(); e
        .hasMoreElements();) {
      final String prop = (String) e.nextElement();

      fileprops.setProperty(prop, myfileprops.getProperty(prop));
    }
    return index + 1;
  }

  /**
   * Runs the main processing loop.
   * @param table The table to operate on.
   * @param db The binding to use.
   */
  private static void mainLoop(String table, DB db) {
    final BufferedReader br =
        new BufferedReader(new InputStreamReader(System.in));

    for (;;) {
      // get user input
      System.out.print("> ");

      String input = null;

      try {
        input = br.readLine();
      } catch (final IOException e) {
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

      final String[] tokens = input.split(" ");

      final long st = System.currentTimeMillis();
      // handle commands
      if (tokens[0].compareTo("table") == 0) {
        if (tokens.length == 1) {
          System.out.println("Using table \"" + table + "\"");
        } else if (tokens.length == 2) {
          table = tokens[1];
          System.out.println("Using table \"" + table + "\"");
        } else {
          System.out.println("Error: syntax is \"table tablename\"");
        }
      } else if (tokens[0].compareTo("read") == 0) {
        if (tokens.length == 1) {
          System.out.println(
              "Error: syntax is \"read keyname " + "[field1 field2 ...]\"");
        } else {
          Set<String> fields = null;

          if (tokens.length > 2) {
            fields = new HashSet<String>();

            for (int i = 2; i < tokens.length; i++) {
              fields.add(tokens[i]);
            }
          }

          final HashMap<String, ByteIterator> result =
              new HashMap<String, ByteIterator>();
          final Status ret = db.read(table, tokens[1], fields, result);
          System.out.println("Return code: " + ret.getName());
          for (final Map.Entry<String, ByteIterator> ent : result.entrySet()) {
            System.out.println(ent.getKey() + "=" + ent.getValue());
          }
        }
      } else if (tokens[0].compareTo("scan") == 0) {
        if (tokens.length < 3) {
          System.out.println("Error: syntax is \"scan keyname scanlength "
              + "[field1 field2 ...]\"");
        } else {
          Set<String> fields = null;

          if (tokens.length > 3) {
            fields = new HashSet<String>();

            for (int i = 3; i < tokens.length; i++) {
              fields.add(tokens[i]);
            }
          }

          final Vector<HashMap<String, ByteIterator>> results =
              new Vector<HashMap<String, ByteIterator>>();
          final Status ret = db.scan(table, tokens[1], 
              Integer.parseInt(tokens[2]), fields, results);
          System.out.println("Return code: " + ret.getName());
          int record = 0;
          if (results.size() == 0) {
            System.out.println("0 records");
          } else {
            System.out.println("--------------------------------");
          }
          for (final HashMap<String, ByteIterator> result : results) {
            System.out.println("Record " + (record++));
            for (final Map.Entry<String, ByteIterator> ent : result
                .entrySet()) {
              System.out.println(ent.getKey() + "=" + ent.getValue());
            }
            System.out.println("--------------------------------");
          }
        }
      } else if (tokens[0].compareTo("update") == 0) {
        if (tokens.length < 3) {
          System.out.println("Error: syntax is \"update keyname name1=value1 "
              + "[name2=value2 ...]\"");
        } else {
          final HashMap<String, ByteIterator> values =
              new HashMap<String, ByteIterator>();

          for (int i = 2; i < tokens.length; i++) {
            final String[] nv = tokens[i].split("=");
            values.put(nv[0], new StringByteIterator(nv[1]));
          }

          final Status ret = db.update(table, tokens[1], values);
          System.out.println("Return code: " + ret.getName());
        }
      } else if (tokens[0].compareTo("insert") == 0) {
        if (tokens.length < 3) {
          System.out.println("Error: syntax is \"insert keyname name1=value1 "
              + "[name2=value2 ...]\"");
        } else {
          final HashMap<String, ByteIterator> values =
              new HashMap<String, ByteIterator>();

          for (int i = 2; i < tokens.length; i++) {
            final String[] nv = tokens[i].split("=");
            values.put(nv[0], new StringByteIterator(nv[1]));
          }

          final Status ret = db.insert(table, tokens[1], values);
          System.out.println("Return code: " + ret.getName());
        }
      } else if (tokens[0].compareTo("delete") == 0) {
        if (tokens.length != 2) {
          System.out.println("Error: syntax is \"delete keyname\"");
        } else {
          final Status ret = db.delete(table, tokens[1]);
          System.out.println("Return code: " + ret.getName());
        }
      } else {
        System.out.println("Error: unknown command \"" + tokens[0] + "\"");
      }

      System.out.println((System.currentTimeMillis() - st) + " ms");

    }
  }

  /**
   * Sets a single command line property.
   * 
   * @param args
   *          The command args.
   * @param argindex
   *          The current argument index.
   * @param props
   *          The property object to update.
   * @return The new argument offset.
   */
  private static int setProp(final String[] args, int argindex,
      Properties props) {
    int index = argindex + 1;
    if (index >= args.length) {
      usageMessage();
      System.exit(0);
    }
    final int eq = args[index].indexOf('=');
    if (eq < 0) {
      usageMessage();
      System.exit(0);
    }

    final String name = args[index].substring(0, eq);
    final String value = args[index].substring(eq + 1);
    props.put(name, value);
    return index + 1;
  }

  /**
   * Hidden Constructor.
   */
  private CommandLine() {
    // Nothing.
  }
}
