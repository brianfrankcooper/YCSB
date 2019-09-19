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
package site.ycsb.db.voltdb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/**
 * Utility class to build the YCSB schema.
 * 
 */
public final class YCSBSchemaBuilder {

  private static final String PROCEDURE_GET_WAS_NOT_FOUND = "Procedure Get was not found";

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final String createTableDDL = "CREATE TABLE Store (keyspace VARBINARY(128)  NOT NULL\n"
      + ",   key      VARCHAR(128)    NOT NULL,   value    VARBINARY(2056) NOT NULL\n"
      + ",   PRIMARY KEY (key, keyspace));";

  private final String partitionTableDDL = "PARTITION TABLE Store ON COLUMN key;\n";

  private final String createGetDDL = "CREATE PROCEDURE Get PARTITION  ON TABLE Store COLUMN key PARAMETER 1\n"
      + "AS SELECT value FROM Store WHERE keyspace = ? AND key = ?;";

  private final String createPutDDL = "CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key PARAMETER 1\n"
      + "FROM CLASS site.ycsb.db.voltdb.procs.Put;";

  private final String createScanDDL = "CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key \n"
      + "FROM CLASS site.ycsb.db.voltdb.procs.Scan;";

  private final String createScanAllDDL = "CREATE PROCEDURE \n" + "FROM CLASS site.ycsb.db.voltdb.procs.ScanAll;";

  private final String[] ddlStatements = {createTableDDL, partitionTableDDL };

  private final String[] procStatements = {createGetDDL, createPutDDL, createScanDDL, createScanAllDDL };

  private final String[] jarFiles = {"Put.class", "Scan.class", "ScanAll.class", "ByteWrapper.class" };

  private final String jarFileName = "ycsb-procs.jar";

  private Logger logger = LoggerFactory.getLogger(YCSBSchemaBuilder.class);

  /**
   * Utility class to build the YCSB schema.
   * 
   * @author srmadscience / VoltDB
   *
   */
  YCSBSchemaBuilder() {
    super();
  }

  /**
   * See if we think YCSB Schema already exists...
   * 
   * @return true if the 'Get' procedure exists and takes one string as a
   *         parameter.
   */
  public boolean schemaExists(Client voltClient) {

    final String testString = "Test";
    boolean schemaExists = false;

    try {
      ClientResponse response = voltClient.callProcedure("Get", testString.getBytes(UTF8), testString);
      
      if (response.getStatus() == ClientResponse.SUCCESS) {
        // YCSB Database exists...
        schemaExists = true;
      } else {
        // If we'd connected to a copy of VoltDB without the schema and tried to call Get
        // we'd have got a ProcCallException
        logger.error("Error while calling schemaExists(): " + response.getStatusString());
        schemaExists = false;
      }
    } catch (ProcCallException pce) {
      schemaExists = false;
      
      // Sanity check: Make sure we've got the *right* ProcCallException...
      if (!pce.getMessage().equals(PROCEDURE_GET_WAS_NOT_FOUND)) {
        logger.error("Got unexpected Exception while calling schemaExists()", pce);
      }

    } catch (Exception e) {
      logger.error("Error while creating classes.", e);
      schemaExists = false;
    }

    return schemaExists;
  }

  /**
   * Load classes and DDL required by YCSB.
   * 
   * @throws Exception
   */
  public synchronized void loadClassesAndDDLIfNeeded(Client voltClient) throws Exception {
    
    if (schemaExists(voltClient)) {
      return;
    }
    
    File tempDir = Files.createTempDirectory("voltdbYCSB").toFile();
    
    if (!tempDir.canWrite()) {
      throw new Exception("Temp Directory (from Files.createTempDirectory()) '" 
            + tempDir.getAbsolutePath() + "' is not writable"); 
    }
  
    ClientResponse cr;

    for (int i = 0; i < ddlStatements.length; i++) {
      try {
        cr = voltClient.callProcedure("@AdHoc", ddlStatements[i]);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
          throw new Exception("Attempt to execute '" + ddlStatements[i] + "' failed:" + cr.getStatusString());
        }
        logger.info(ddlStatements[i]);
      } catch (Exception e) {

        if (e.getMessage().indexOf("object name already exists") > -1) {
          // Someone else has done this...
          return;
        }

        throw (e);
      }
    }

    logger.info("Creating JAR file in " + tempDir + File.separator + jarFileName);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream newJarFile = new JarOutputStream(new FileOutputStream(tempDir + File.separator + jarFileName),
        manifest);

    for (int i = 0; i < jarFiles.length; i++) {
      InputStream is = getClass().getResourceAsStream("/site/ycsb/db/voltdb/procs/" + jarFiles[i]);
      add("site/ycsb/db/voltdb/procs/" + jarFiles[i], is, newJarFile);
    }

    newJarFile.close();
    File file = new File(tempDir + File.separator + jarFileName);

    byte[] jarFileContents = new byte[(int) file.length()];
    FileInputStream fis = new FileInputStream(file);
    fis.read(jarFileContents);
    fis.close();
    logger.info("Calling @UpdateClasses to load JAR file containing procedures");

    cr = voltClient.callProcedure("@UpdateClasses", jarFileContents, null);
    if (cr.getStatus() != ClientResponse.SUCCESS) {
      throw new Exception("Attempt to execute UpdateClasses failed:" + cr.getStatusString());
    }

    for (int i = 0; i < procStatements.length; i++) {
      logger.info(procStatements[i]);
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
