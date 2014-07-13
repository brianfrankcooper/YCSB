/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
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
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.CoreWorkload;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Tested with Cassandra 2.0, CQL client for YCSB framework
 *
 * In CQLSH, create keyspace and table.  Something like:
 *
 * create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1 };
 * create table ycsb.usertable (
 *     y_id varchar primary key,
 *     field0 varchar,
 *     field1 varchar,
 *     field2 varchar,
 *     field3 varchar,
 *     field4 varchar,
 *     field5 varchar,
 *     field6 varchar,
 *     field7 varchar,
 *     field8 varchar,
 *     field9 varchar);
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

    private static Cluster cluster = null;
    private static Session session = null;

    private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

    public static final int OK = 0;
    public static final int ERR = -1;

    public static final String YCSB_KEY = "y_id";
    public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
    public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static boolean _debug = false;
    private static boolean readallfields;
    private static boolean writeallfields;

    private static PreparedStatement insertStatement = null;
    private static PreparedStatement selectStatement = null;
    private static Map<String, PreparedStatement> selectStatements = null;
    private static PreparedStatement scanStatement = null;
    private static Map<String, PreparedStatement> scanStatements = null;
    private static PreparedStatement deleteStatement = null;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public synchronized void init() throws DBException {
        //Check if the cluster has already been initialized
        if (cluster != null) {
            return;
        }

        try {

            _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

            if (getProperties().getProperty("hosts") == null) {
                throw new DBException("Required property \"hosts\" missing for CassandraClient");
            }
            String hosts[] = getProperties().getProperty("hosts").split(",");
            String port = getProperties().getProperty("port", "9042");
            if (port == null) {
                throw new DBException("Required property \"port\" missing for CassandraClient");
            }

            String username = getProperties().getProperty(USERNAME_PROPERTY);
            String password = getProperties().getProperty(PASSWORD_PROPERTY);

            String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

            readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
            writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
            readallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, CoreWorkload.READ_ALL_FIELDS_PROPERTY_DEFAULT));
            writeallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.WRITE_ALL_FIELDS_PROPERTY, CoreWorkload.WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

            // public void connect(String node) {}
            if ((username != null) && !username.isEmpty()) {
                cluster = Cluster.builder()
                                 .withCredentials(username, password)
                                 .withPort(Integer.valueOf(port))
                                 .addContactPoints(hosts).build();
            }
            else {
                cluster = Cluster.builder()
                                 .withPort(Integer.valueOf(port))
                                 .addContactPoints(hosts).build();
            }

            //Update number of connections based on threads
            int threadcount = Integer.parseInt(getProperties().getProperty("threadcount","2"));
            cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, threadcount);

            //Set connection timeout 3min (default is 5s)
            cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(3*60*1000);
            //Set read (execute) timeout 3min (default is 12s)
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(3*60*1000);

            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

            for (Host discoveredHost : metadata.getAllHosts()) {
                System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                                  discoveredHost.getDatacenter(),
                                  discoveredHost.getAddress(),
                                  discoveredHost.getRack());
            }

            session = cluster.connect(keyspace);

            // build prepared statements.
            buildStatements();
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    private void buildStatements() {

        Properties p = getProperties();
        int fieldCount = Integer.parseInt(p.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        String fieldPrefix = p.getProperty(CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
        String table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);

        // Insert and Update statement
        Insert is = QueryBuilder.insertInto(table);
        is.value(YCSB_KEY, QueryBuilder.bindMarker());
        for (int i = 0; i < fieldCount; i++)
            is.value(fieldPrefix + i, QueryBuilder.bindMarker());
        insertStatement = session.prepare(is);

        // Delete statement
        deleteStatement = session.prepare(QueryBuilder.delete().from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));

        if (!readallfields)
        {
            // Select statements
            selectStatements = new ConcurrentHashMap<String,PreparedStatement>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                Select ss = QueryBuilder.select(fieldPrefix + i).from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).limit(1);
                selectStatements.put(fieldPrefix + i, session.prepare(ss));
            }

            // Scan statements
            scanStatements = new ConcurrentHashMap<String,PreparedStatement>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                String initialStmt = QueryBuilder.select(fieldPrefix + i).from(table).toString();
                String scanStmt = getScanQueryString().replaceFirst("_", initialStmt.substring(0, initialStmt.length()-1));
                scanStatements.put(fieldPrefix + i, session.prepare(scanStmt));
            }
        }
        else
        {
            // Select statement
            Select ss = QueryBuilder.select().all().from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).limit(1);
            selectStatement = session.prepare(ss);
            selectStatement.setConsistencyLevel(readConsistencyLevel);

            // Scan statement
            String initialStmt = QueryBuilder.select().all().from(table).toString();
            String scanStmt = getScanQueryString().replaceFirst("_", initialStmt.substring(0, initialStmt.length()-1));
            scanStatement = session.prepare(scanStmt);
            scanStatement.setConsistencyLevel(readConsistencyLevel);
        }
    }

    private String getScanQueryString()
    {
        StringBuilder scanStmt = new StringBuilder();
        return scanStmt.append("_")
                       .append(" WHERE ")
                       .append(QueryBuilder.token(YCSB_KEY))
                       .append(" >= ")
                       .append("token(")
                       .append(QueryBuilder.bindMarker())
                       .append(")")
                       .append(" LIMIT ")
                       .append(QueryBuilder.bindMarker()).toString();
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a Map.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int readAll(String table, String key, Map<String, ByteIterator> result)
    {
        BoundStatement bs = selectStatement.bind(key);
        return read(key, result, bs);
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int readOne(String table, String key, String field, Map<String, ByteIterator> result) {
        BoundStatement bs = selectStatements.get(field).bind(key);
        return read(key, result, bs);
    }

    public int read(String key, Map<String, ByteIterator> result, BoundStatement bs) {

        try {

            if (_debug)
                System.out.println(bs.preparedStatement().getQueryString());

            ResultSet rs = session.execute(bs);

            //Should be only 1 row
            if (!rs.isExhausted()) {
                Row row = rs.one();
                ColumnDefinitions cd = row.getColumnDefinitions();
                
                for (ColumnDefinitions.Definition def : cd) {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null) {
                        result.put(def.getName(),
                            new ByteArrayByteIterator(val.array()));
                    }
                    else {
                        result.put(def.getName(), null);
                    }
                }

            }

            return OK;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error reading key: " + key);
            return ERR;
        }

    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a Map.
     *
     * Cassandra CQL uses "token" method for range scan which doesn't always
     * yield intuitive results.
     *
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field The field to read
     * @param result A List of Maps, where each Map is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result)
    {
        BoundStatement bs = scanStatements.get(field).bind(startkey, recordcount);
        return scan(startkey, result, bs);
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a Map.
     *
     * Cassandra CQL uses "token" method for range scan which doesn't always
     * yield intuitive results.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param result A List of Maps, where each Map is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result)
    {
        BoundStatement bs = scanStatement.bind(startkey, recordcount);
        return scan(startkey, result, bs);
    }

    public int scan(String startkey, List<Map<String, ByteIterator>> result, BoundStatement bs) {

        try {

            if (_debug)
                System.out.println(bs.preparedStatement().getQueryString());

            ResultSet rs = session.execute(bs);

            HashMap<String, ByteIterator> tuple;
            while (!rs.isExhausted()) {
                Row row = rs.one();
                tuple = new HashMap<String, ByteIterator> ();

                ColumnDefinitions cd = row.getColumnDefinitions();
                
                for (ColumnDefinitions.Definition def : cd) {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null) {
                        tuple.put(def.getName(),
                            new ByteArrayByteIterator(val.array()));
                    }
                    else {
                        tuple.put(def.getName(), null);
                    }
                }

                result.add(tuple);
            }

            return OK;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error scanning with startkey: " + startkey);
            return ERR;
        }

    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param field The field to update
     * @param value The value to update in the key record
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int updateOne(String table, String key, String field, ByteIterator value)
    {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put(field, value);
        return insert(table, key, values);
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.
     */
    @Override
    public int updateAll(String table, String key, Map<String,ByteIterator> values)
    {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values Map will be written into the record with the specified record
     * key.
     *
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A Map of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {

        try {

            List<String> vals = new ArrayList<String>(values.size() + 1);
            vals.add(key);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet())
                vals.add(entry.getValue().toString());

            if (_debug)
                System.out.println(insertStatement.getQueryString());

            session.execute(insertStatement.bind(vals.toArray()));

            return OK;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ERR;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int delete(String table, String key) {

        try {
            if (_debug)
                System.out.println(deleteStatement.getQueryString());

            session.execute(deleteStatement.bind(key));

            return OK;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error deleting key: " + key);
        }

        return ERR;
    }

}
