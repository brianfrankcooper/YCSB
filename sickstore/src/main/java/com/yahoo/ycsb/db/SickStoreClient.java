/**
 * Copyright (c) 2015, Yahoo!, Inc. All rights reserved.
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

import java.io.IOException;
import java.util.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import de.unihamburg.sickstore.backend.Version;
import de.unihamburg.sickstore.database.SickClient;
import de.unihamburg.sickstore.database.WriteConcern;
import de.unihamburg.sickstore.database.messages.exception.DatabaseException;

/**
 * SickStore client for YCSB.
 * 
 * Properties to set:
 * sickstore.url=localhost
 * sickstore.port=54000
 * sickstore.timeout=1000
 * 
 * @author Wolfram Wingerath, Steffen Friedrich
 */
public class SickStoreClient extends DB {

    /** status code indicating that an operation failed */
    private static final int STATUS_FAIL = -1;

    /** status code indicating everything went fine */
    private static final int STATUS_OK = 0;

    /**
     * status code indicating that a value could not be retrieved, because it
     * was expected to be of type <code>String</code>, but wasn't
     */
    private static final int STATUS_WRONGTYPE_STRINGEXPECTED = -2;

    private SickClient client = null;

    private WriteConcern writeConcern;

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        client.disconnect();
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        try {
            client.delete(table, key, writeConcern);
            return STATUS_OK;
        } catch (Exception e) {
            e.printStackTrace();
            return STATUS_FAIL;
        }
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        // initialize SickStore driver
        Properties props = getProperties();
        int timeout = Integer.parseInt(props.getProperty("sickstore.timeout", "1000"));
        String url = props.getProperty("sickstore.url", "localhost");
        int port = Integer.parseInt(props.getProperty("sickstore.port", "54000"));

        // configure write concern
        writeConcern = new WriteConcern();
        String ack = props.getProperty("sickstore.write_concern.ack", "1");
        try {
            writeConcern.setReplicaAcknowledgement(Integer.parseInt(ack));
        } catch (NumberFormatException e) {
            // no number given, assume it is a tag set
            writeConcern.setReplicaAcknowledgementTagSet(ack);
        }

        String journaling = props.getProperty("sickstore.write_concern.journaling", "false");
        if (journaling.equals("true")) {
            writeConcern.setJournaling(true);
        }

        try {
            client = new SickClient(timeout, url, port);
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBException("Could not connect to server!");
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) { 
        try { 
            Version version = new Version();
            for (String k : values.keySet()) { 
                Object v = values.get(k).toString();
                version.put(k, (String) v); 
            }
            client.insert(table, key, version, writeConcern);
            return STATUS_OK;
        } catch (Exception e) {
            e.printStackTrace();
            return STATUS_FAIL;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            Version version = null;
            try {
                version = client.read(table, key, fields);
            } catch (DatabaseException e) {
                e.printStackTrace();
                return STATUS_FAIL;
            }

            Object value = null;
            for (String k : version.getValues().keySet()) {
                value = version.get(k);
                if (value instanceof String) {
                    result.put(k, new StringByteIterator((String) value));
                } else {
                    return STATUS_WRONGTYPE_STRINGEXPECTED;
                }
            }
            return STATUS_OK;
        } catch (Exception e) {
            e.printStackTrace();
            return STATUS_FAIL;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            HashMap<String, ByteIterator> resultMap = null;
            List<Version> versions = null;
            Version version = null;
            Object value = null;

            try {
                versions = client.scan(table, startkey, recordcount, fields,
                        true);
            } catch (DatabaseException e) {
                e.printStackTrace();
                return STATUS_FAIL;
            }

            for (int i = 0; i < versions.size(); i++) {
                version = versions.get(i);
                resultMap = new HashMap<String, ByteIterator>();
                for (String k : fields) {
                    value = version.get(k);
                    if (value instanceof String) {
                        resultMap
                                .put(k, new StringByteIterator((String) value));
                    } else {
                        return STATUS_WRONGTYPE_STRINGEXPECTED;
                    }
                }

                result.add(resultMap);
            }
            return STATUS_OK;
        } catch (Exception e) {
            e.printStackTrace();
            return STATUS_FAIL;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write.
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            Version version = new Version();
            for (String column : values.keySet()) { 
                version.put(column, values.get(column).toString());
            }
            if (client.update(table, key, version, writeConcern)) {
                return STATUS_OK;
            } else {
                return STATUS_FAIL;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return STATUS_FAIL;
        }
    }
}
