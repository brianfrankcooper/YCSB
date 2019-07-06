/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.db.voltdb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

/**
 * Utility class to build the YCSB schema.
 * 
 */
public final class YCSBSchemaBuilder {

  private static YCSBSchemaBuilder instance = null;

  private final String createTableDDL = "CREATE TABLE Store (keyspace VARBINARY(128)  NOT NULL\n"
      + ",   key      VARCHAR(128)    NOT NULL,   value    VARBINARY(2056) NOT NULL\n"
      + ",   PRIMARY KEY (key, keyspace));";

  private final String partitionTableDDL = "PARTITION TABLE Store ON COLUMN key;\n";

  private final String createGetDDL = "CREATE PROCEDURE Get PARTITION  ON TABLE Store COLUMN key PARAMETER 1\n"
      + "AS SELECT value FROM Store WHERE keyspace = ? AND key = ?;";

  private final String createPutDDL = "CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key PARAMETER 1\n"
      + "FROM CLASS com.yahoo.ycsb.db.voltdb.procs.Put;";

  private final String createScanDDL = "CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key \n"
      + "FROM CLASS com.yahoo.ycsb.db.voltdb.procs.Scan;";

  private final String createScanAllDDL = "CREATE PROCEDURE \n" + "FROM CLASS com.yahoo.ycsb.db.voltdb.procs.ScanAll;";

  private final String[] ddlStatements = {createTableDDL, partitionTableDDL };

  private final String[] procStatements = {createGetDDL, createPutDDL, createScanDDL, createScanAllDDL };

  private final String[] jarFiles = {"Put.class", "Scan.class", "ScanAll.class", "ByteWrapper.class" };

  private final String jarFileName = "ycsb-procs.jar";

  private Client voltClient;

  /**
   * Utility class to build the YCSB schema.
   * 
   * @author srmadscience / VoltDB
   *
   */
  private YCSBSchemaBuilder(Client c) {
    super();
    this.voltClient = c;
  }

  /**
   * We use a single instance of YCSBSchemaBuilder because YCSB can spawn an
   * arbitrary number of threads that will each try and call init(). Our single
   * YCSBSchemaBuilder instance remembers requests to load classes and DDL and
   * only does it once.
   * 
   * @param c
   * @return YCSBSchemaBuilder instance
   */
  public static YCSBSchemaBuilder getInstance(Client c) {

    if (instance == null) {
      instance = new YCSBSchemaBuilder(c);
    }

    return instance;
  }

  /**
   * Load classes and DDL required by YCSB.
   * 
   * @throws Exception
   */
  public synchronized void loadClassesAndDDL(File baseDir) throws Exception {

    ClientResponse cr;

    for (int i = 0; i < ddlStatements.length; i++) {
      try {
        cr = voltClient.callProcedure("@AdHoc", ddlStatements[i]);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
          throw new Exception("Attempt to execute '" + ddlStatements[i] + "' failed:" + cr.getStatusString());
        }
        System.out.println(ddlStatements[i]);
      } catch (Exception e) {
        
        if (e.getMessage().indexOf("object name already exists") > -1) {
          // Someone else has done this...
          return;
        }

        throw (e);
      }
    }

    System.out.println("Creating JAR file in " + baseDir + File.separator + jarFileName);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream newJarFile = new JarOutputStream(new FileOutputStream(baseDir + File.separator + jarFileName),
        manifest);

    for (int i = 0; i < jarFiles.length; i++) {
      InputStream is = getClass().getResourceAsStream("/org/voltdbycsb/procs/" + jarFiles[i]);
      add("org/voltdbycsb/procs/" + jarFiles[i], is, newJarFile);
    }

    newJarFile.close();
    File file = new File(baseDir + File.separator + jarFileName);

    byte[] jarFileContents = new byte[(int) file.length()];
    FileInputStream fis = new FileInputStream(file);
    fis.read(jarFileContents);
    fis.close();
    System.out.println("Calling @UpdateClasses to load JAR file containing procedures");

    cr = voltClient.callProcedure("@UpdateClasses", jarFileContents, null);
    if (cr.getStatus() != ClientResponse.SUCCESS) {
      throw new Exception("Attempt to execute UpdateClasses failed:" + cr.getStatusString());
    }

    for (int i = 0; i < procStatements.length; i++) {
      System.out.println(procStatements[i]);
      cr = voltClient.callProcedure("@AdHoc", procStatements[i]);
      if (cr.getStatus() != ClientResponse.SUCCESS) {
        throw new Exception("Attempt to execute '" + procStatements[i] + "' failed:" + cr.getStatusString());
      }
    }

  }

  /**
   * Add an entry to our JAR file.
   * 
   * @param fileName
   * @param source
   * @param target
   * @throws IOException
   */
  private void add(String fileName, InputStream source, JarOutputStream target) throws IOException {
    BufferedInputStream in = null;
    try {

      JarEntry entry = new JarEntry(fileName.replace("\\", "/"));
      entry.setTime(System.currentTimeMillis());
      target.putNextEntry(entry);
      in = new BufferedInputStream(source);

      byte[] buffer = new byte[1024];
      while (true) {
        int count = in.read(buffer);
        if (count == -1) {
          break;
        }

        target.write(buffer, 0, count);
      }
      target.closeEntry();
    } finally {
      if (in != null) {
        in.close();
      }

    }
  }

}
