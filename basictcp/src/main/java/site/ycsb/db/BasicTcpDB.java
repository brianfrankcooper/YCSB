/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Basic TCP based DB client binding for YCSB.
 *
 * Based off of the S3 client.
 */
package site.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.io.*;
import java.util.*;
import java.net.*;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * Basic TCP based DB client for YCSB framework.
 *
 */
public class BasicTcpDB extends DB {
  public static final String HOST = "basictcpdb.host";
  public static final String HOST_DEFAULT = "127.0.0.1";

  public static final String PORT = "basictcpdb.port";
  public static final String PORT_DEFAULT = "54321";

  private Socket socket;
  private String host;
  private Integer port;
  private PrintWriter outStream;
  private BufferedReader inStream;

  protected static final ThreadLocal<StringBuilder> TL_STRING_BUILDER =
      new ThreadLocal<StringBuilder>() {
      @Override
      protected StringBuilder initialValue() {
        return new StringBuilder();
      }
    };

  protected StringBuilder getStringBuilder() {
    StringBuilder sb = TL_STRING_BUILDER.get();
    sb.setLength(0);
    return sb;
  }

  /**
   * Initialize.
   */
  @Override
  public void init() throws DBException {
    host = getProperties().getProperty(HOST, HOST_DEFAULT);
    port = Integer.parseInt(getProperties().getProperty(PORT, PORT_DEFAULT));

    try {
      this.socket = new Socket(host, port);
      this.outStream = new PrintWriter(socket.getOutputStream(), true);
      this.inStream =
        new BufferedReader(new InputStreamReader(socket.getInputStream()));
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      this.socket.close();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * Insert.
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {

    StringBuilder sb = this.getStringBuilder();
    sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
    if (values != null) {
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        sb.append(entry.getKey()).append("=").append(entry.getValue())
          .append(" ");
      }
    }

    sb.append("]");

    this.outStream.write(sb.toString());

    return Status.OK;
  }

  /**
   * Read.
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {

    StringBuilder sb = this.getStringBuilder();
    sb.append("READ ").append(table).append(" ").append(key).append(" [ ");
    if (fields != null) {
      for (String f : fields) {
        sb.append(f).append(" ");
      }
    } else {
      sb.append("<all fields>");
    }

    sb.append("]");

    this.outStream.write(sb.toString());

    return Status.OK;
  }

  /**
   * Update.
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {

    StringBuilder sb = this.getStringBuilder();
    sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
    if (values != null) {
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        sb.append(entry.getKey()).append("=").append(entry.getValue())
          .append(" ");
      }
    }
    sb.append("]");

    this.outStream.write(sb.toString());

    return Status.OK;
  }

  /**
   * Delete.
   */
  @Override
  public Status delete(String table, String key) {
    StringBuilder sb = this.getStringBuilder();
    sb.append("DELETE ").append(table).append(" ").append(key);

    this.outStream.write(sb.toString());

    return Status.OK;
  }

  /**
   * Scan.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    StringBuilder sb = this.getStringBuilder();
    sb.append("SCAN ").append(table).append(" ").append(startkey).append(" ")
      .append(recordcount).append(" [ ");
    if (fields != null) {
      for (String f : fields) {
        sb.append(f).append(" ");
      }
    } else {
      sb.append("<all fields>");
    }

    sb.append("]");

    this.outStream.write(sb.toString());

    return Status.OK;
  }
}
