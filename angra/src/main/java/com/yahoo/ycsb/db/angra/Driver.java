/**
* Copyright (c) 2016 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db.angra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
*Class with the interface to TCP connection of Angra-DB.
*/

public class Driver {

  private static final Logger LOGGER = Logger.getLogger(Driver.class.getName());

  private Socket client;
  private String host;
  private String port;

  public Driver(String host, String port) throws Exception {
    this.host = host;
    this.port = port;
  }

  /**
  * Get a TCP connection with angraDb.
  *
  */
  public void getTcpConnection(){
    LOGGER.log(Level.INFO, "getTCPConnection: Trying to connect to " + host + ":" + port + "....\n");
    try {
      if (client == null) {
        client = new Socket(host, Integer.parseInt(port));
        LOGGER.log(Level.INFO, "getTCPConnection: Connected to " + host + ":" + port + ".\n");
      }
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "getTCPConnection: Could not create a TCP connection " +
          "due to the following exception: ", e);
      //throw new Exception();
    }

  }

  /**
  * Creates a database schema and get a response.
  *
  * @param dbName is the name of schema.
  *
  */
  public void createDatabase(String dbName) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("create_db " + dbName);

    if (reader.readLine().contains("{ok")) {
      LOGGER.log(Level.INFO, "createDatabase: Database " + dbName + " created with success.\n");
    } else {
      LOGGER.log(Level.SEVERE, "createDatabase: Could not create the database " + dbName+ "\n");
    }
  }

  /**
  * Connect with a specific database schema.
  *
  * @param dbName is the name of schema.
  *
  */
  public void connectToDatabase(String dbName) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("connect " + dbName);

    if (reader.readLine().contains("Database set to")) {
      LOGGER.log(Level.INFO, "connectToDatabase: Connected to " + dbName + " database.\n");
    } else {
      LOGGER.log(Level.SEVERE, "connectToDatabase: Could not connect the database " + dbName+ "\n");
    }
  }

  /**
  * Insert a document.
  *
  * @param document is a value to be inserted on database schema.
  */

  public String save(String document) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("save " + document);

    String key = reader.readLine();

    if (key.contains("\"")) {
      LOGGER.log(Level.INFO, "save: save key " + key + " to database.\n");
    } else {
      LOGGER.log(Level.SEVERE, "save: Could not save the database " + key+
          " with following document: "+ document+ "\n");
    }
    return key.replaceAll("\"", "");
  }
  /**
  * Insert a document with a given Id.
  *
  * @param key id de desired Id.
  * @param document is a value to be inserted on database schema.
  */
  public String saveKey(String key, String document) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("save_key " + key + " " + document);

    String recKey = reader.readLine();
    if (recKey.contains("\"")) {
      LOGGER.log(Level.INFO, "saveKey: save key " + recKey + " to database.\n");
    } else {
      LOGGER.log(Level.SEVERE, "saveKey: Could not save the database " + recKey+
          " with following document: "+ document+ "\n");
    }
    return recKey.replaceAll("\"", "");
  }

  /**
  * Search a document.
  *
  * @param key is a key of the requested document.
  */
  public String lookup(String key) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("lookup " + key);
    String resp = reader.readLine();

    if (resp.equals("not_found") || resp.equals("{badrecord,state}")) {
      LOGGER.log(Level.SEVERE, "lookup: Could not find " + key +
          " in the database, received following response: "+ resp + "\n");
    } else {
      LOGGER.log(Level.INFO, "lookup: lookup key " + key + " to database and found:" + resp + "\n");
    }
    return resp;
  }

  /**
  * Update a document.
  *
  * @param key is a key of the requested document.
  * @param newDocument is a new value for document.
  */
  public String update(String key, String newDocument) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("update " + key + " " + newDocument);
    String resp = reader.readLine();
    if (resp.startsWith("ok")) {
      LOGGER.log(Level.INFO, "updade: update on key " + key + " done in database.\n");
    } else {
      LOGGER.log(Level.SEVERE, "updade: Could not updade the " + key+
          " key in database, with following document: "+ newDocument+ "\n");
    }
    return resp;
  }

  /**
  * Delete a document.
  *
  * @param key is a key of the document to be deleted. {error,keyUnused}

  */
  public String delete(String key) throws IOException {
    getTcpConnection();
    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
    InputStreamReader in = new InputStreamReader(client.getInputStream());
    BufferedReader reader = new BufferedReader(in);

    out.println("delete " + key);
    String resp = reader.readLine();
    if (resp.startsWith("ok")) {
      LOGGER.log(Level.INFO, "delete: delete on key " + key + " done in database.\n");
    } else {
      LOGGER.log(Level.SEVERE, "delete: Could not delete the " + key+
          " key in database, with following response: "+ resp+ "\n");
    }
    return resp;
  }

  public Socket getClient() {
    return client;
  }

  public void setClient(Socket c) {
    this.client = c;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String h) {
    this.host = h;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String p) {
    this.port = p;
  }

}
