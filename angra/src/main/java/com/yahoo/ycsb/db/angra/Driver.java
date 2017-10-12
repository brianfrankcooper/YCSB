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

public class Driver {

	private static final Logger LOGGER = Logger.getLogger(Driver.class.getName());

	private Socket client;
	private String host;
	private int port;

	public Driver(String host, int port) throws Exception {
		this.host = host;
		this.port = port;
	}

	/**
	 * Get a TCP connection with angraDb.
	 *
	 */
	public void getTcpConnection() throws Exception {
		LOGGER.log(Level.INFO, "\nTrying to connect to " + host + ":" + port + "....\n");

		try {
			client = new Socket(host, port);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "\nCould not create a TCP connection due to the following exception:\n", e);
			throw new Exception();
		}
		LOGGER.log(Level.INFO, "\nConnected to " + host + ":" + port + ".\n");
	}

	/**
	 * Creates a database schema and get a response.
	 *
	 * @param dbName is the name of schema.
	 *
	 */
	public void createDatabase(String dbName) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("create_db " + dbName);

		if (reader.readLine().contains("{ok")) {
			LOGGER.log(Level.INFO, "\nDatabase " + dbName + " created with success.\n");
		} else {
			LOGGER.log(Level.SEVERE, "\nCould not create the database!\n" + dbName);
		}
	}

	/**
	 * Connect with a specific database schema.
	 *
	 * @param dbName is the name of schema.
	 *
	 */
	public void connectToDatabase(String dbName) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("connect " + dbName);
		LOGGER.log(Level.INFO, "\n" + reader.readLine() + "\n");
	}

	/**
	 * Insert a document.
	 *
	 * @param document is a value to be inserted on database schema.
	 */

	public String save(String document) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("save " + document);

		String key = reader.readLine();

		LOGGER.log(Level.INFO, "\n" + key + "\n");
		return key.replaceAll("\"", "");
	}
	/**
	 * Insert a document with a given Id.
	 *
	 * @param key id de desired Id.
	 * @param document is a value to be inserted on database schema.
	 */
	public String save_key(String key, String document) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("save_key " + key + " " + document);

		String key = reader.readLine();

		LOGGER.log(Level.INFO, "\n" + key + "\n");
		return key.replaceAll("\"", "");
	}

	/**
	 * Search a document.
	 *
	 * @param key is a key of the requested document.
	 */
	public void lookup(String key) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("lookup " + key);
		String resp = reader.readLine();
		LOGGER.log(Level.INFO, "\n" + resp + "\n");
		return resp;
	}

	/**
	 * Update a document.
	 *
	 * @param key is a key of the requested document.
	 * @param newDocument is a new value for document.
	 */
	public String update(String key, String newDocument) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("update " + key + " " + newDocument);
		String resp = reader.readLine();
		LOGGER.log(Level.INFO, "\n" + resp + "\n");
		return resp;
	}

	/**
	 * Delete a document.
	 *
	 * @param key is a key of the document to be deleted.
	 */
	public void delete(String key) throws IOException {
		PrintWriter out = new PrintWriter(client.getOutputStream(), true);
		InputStreamReader in = new InputStreamReader(client.getInputStream());
		BufferedReader reader = new BufferedReader(in);

		out.println("delete " + key);
		String resp = reader.readLine();
		LOGGER.log(Level.INFO, "\n" + resp + "\n");
		return resp;
	}

	public Socket getClient() {
		return client;
	}

	public void setClient(Socket client) {
		this.client = client;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
