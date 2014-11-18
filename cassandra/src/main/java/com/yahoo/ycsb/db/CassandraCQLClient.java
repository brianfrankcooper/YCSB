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
import java.util.Iterator;
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
   create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1 };
   create table ycsb.usertable (
        y_id varchar primary key,
        field0 blob,
        field1 blob,
        field2 blob,
        field3 blob,
        field4 blob,
        field5 blob,
        field6 blob,
        field7 blob,
        field8 blob,
        field9 blob);
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB
{
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

    private static PreparedStatement deleteStatement = null;

    // select and scan statements have two variants; one to select all columns, and one for selecting each individual column
    private static PreparedStatement selectStatement = null;
    private static Map<String, PreparedStatement> selectStatements = null;
    private static PreparedStatement scanStatement = null;
    private static Map<String, PreparedStatement> scanStatements = null;

    // YCSB always inserts a full row, but updates can be either full-row or single-column
    private static PreparedStatement insertStatement = null;
    private static Map<String, PreparedStatement> updateStatements = null;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public synchronized void init() throws DBException
    {
        //Check if the cluster has already been initialized
        if (cluster != null)
            return;

        try
        {
            _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

            if (getProperties().getProperty("hosts") == null)
                throw new DBException("Required property \"hosts\" missing for CassandraClient");

            String hosts[] = getProperties().getProperty("hosts").split(",");
            String port = getProperties().getProperty("port", "9042");
            if (port == null)
                throw new DBException("Required property \"port\" missing for CassandraClient");


            String username = getProperties().getProperty(USERNAME_PROPERTY);
            String password = getProperties().getProperty(PASSWORD_PROPERTY);

            String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

            readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
            writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
            readallfields = Boolean.parseBoolean(getProperties().getProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, CoreWorkload.READ_ALL_FIELDS_PROPERTY_DEFAULT));

            Cluster.Builder builder = Cluster.builder()
                                             .withPort(Integer.valueOf(port))
                                             .addContactPoints(hosts);
            if ((username != null) && !username.isEmpty())
            {
                builder = builder.withCredentials(username, password);
            }
            cluster = builder.build();

            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

            for (Host discoveredHost : metadata.getAllHosts())
            {
                System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                                  discoveredHost.getDatacenter(),
                                  discoveredHost.getAddress(),
                                  discoveredHost.getRack());
            }

            session = cluster.connect(keyspace);

            buildStatements();
        }
        catch (Exception e)
        {
            throw new DBException(e);
        }
    }

    private void buildStatements()
    {
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
        insertStatement.setConsistencyLevel(writeConsistencyLevel);

        // Update statements for updateOne
        updateStatements = new ConcurrentHashMap<String,PreparedStatement>(fieldCount);
        for (int i = 0; i < fieldCount; i++)
        {
            is = QueryBuilder.insertInto(table);
            is.value(YCSB_KEY, QueryBuilder.bindMarker());
            is.value(fieldPrefix + i, QueryBuilder.bindMarker());

            PreparedStatement ps = session.prepare(is);
            ps.setConsistencyLevel(writeConsistencyLevel);
            updateStatements.put(fieldPrefix + i, ps);
        }


        // Delete statement
        deleteStatement = session.prepare(QueryBuilder.delete().from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));
        deleteStatement.setConsistencyLevel(writeConsistencyLevel);

        if (readallfields)
        {
            // Select statement
            String ss = QueryBuilder.select().all().from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).getQueryString();
            selectStatement = session.prepare(ss);
            selectStatement.setConsistencyLevel(readConsistencyLevel);

            // Scan statement
            String initialStmt = QueryBuilder.select().all().from(table).toString();
            String scanStmt = getScanQueryString().replaceFirst("_", initialStmt.substring(0, initialStmt.length()-1));
            scanStatement = session.prepare(scanStmt);
            scanStatement.setConsistencyLevel(readConsistencyLevel);
        }
        else
        {
            // Select statements
            selectStatements = new ConcurrentHashMap<String,PreparedStatement>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                Select ss = QueryBuilder.select(fieldPrefix + i).from(table).where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())).limit(1);
                PreparedStatement ps = session.prepare(ss);
                ps.setConsistencyLevel(readConsistencyLevel);
                selectStatements.put(fieldPrefix + i, ps);
            }

            // Scan statements
            scanStatements = new ConcurrentHashMap<String,PreparedStatement>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                String initialStmt = QueryBuilder.select(fieldPrefix + i).from(table).toString();
                String scanStmt = getScanQueryString().replaceFirst("_", initialStmt.substring(0, initialStmt.length()-1));
                PreparedStatement ps = session.prepare(scanStmt);
                ps.setConsistencyLevel(readConsistencyLevel);
                scanStatements.put(fieldPrefix + i, ps);
            }
        }
    }

    private String getScanQueryString()
    {
        return String.format("_ WHERE %s >= token(%s) LIMIT %s", QueryBuilder.token(YCSB_KEY), QueryBuilder.bindMarker(), QueryBuilder.bindMarker());
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {}

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
    public int readOne(String table, String key, String field, Map<String, ByteIterator> result)
    {
        BoundStatement bs = selectStatements.get(field).bind(key);
        return read(key, result, bs);
    }

    public int read(String key, Map<String, ByteIterator> result, BoundStatement bs)
    {
        try
        {
            if (_debug)
                System.out.println(bs.preparedStatement().getQueryString());

            ResultSet rs = session.execute(bs);
            Row row = rs.one();
            assert row != null : "Key " + key + " was not found; did you run a load job with the correct parameters?";
            for (ColumnDefinitions.Definition def : row.getColumnDefinitions())
            {
                ByteBuffer val = row.getBytesUnsafe(def.getName());
                result.put(def.getName(), val == null ? null : new ByteArrayByteIterator(val.array()));
            }

            return OK;
        }
        catch (Exception e)
        {
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

    public int scan(String startkey, List<Map<String, ByteIterator>> result, BoundStatement bs)
    {
        try
        {
            if (_debug)
                System.out.println(bs.preparedStatement().getQueryString());

            ResultSet rs = session.execute(bs);

            Iterator<Row> iter = rs.iterator();
            while (iter.hasNext())
            {
                Row row = iter.next();

                HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
                for (ColumnDefinitions.Definition def : row.getColumnDefinitions())
                {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    tuple.put(def.getName(), val == null ? null : new ByteArrayByteIterator(val.array()));
                }

                result.add(tuple);
            }

            return OK;
        }
        catch (Exception e)
        {
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
    public int insert(String table, String key, Map<String, ByteIterator> values)
    {
        try
        {
            Object[] vals = new Object[values.size() + 1];
            vals[0] = key;
            int i = 1;
            for (Map.Entry<String, ByteIterator> entry : values.entrySet())
            {
                vals[i++] = ByteBuffer.wrap(entry.getValue().toArray());
            }

            BoundStatement bs = (values.size() == 1 ? updateStatements.get(values.keySet().iterator().next()) : insertStatement).bind(vals);

            if (_debug)
                System.out.println(bs.preparedStatement().getQueryString());

            session.execute(bs);

            return OK;
        }
        catch (Exception e)
        {
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
    public int delete(String table, String key)
    {
        try
        {
            if (_debug)
                System.out.println(deleteStatement.getQueryString());

            session.execute(deleteStatement.bind(key));

            return OK;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error deleting key: " + key);
        }

        return ERR;
    }
}
