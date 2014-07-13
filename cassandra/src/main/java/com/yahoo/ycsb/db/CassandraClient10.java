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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import java.util.*;
import java.nio.ByteBuffer;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.*;


//XXXX if we do replication, fix the consistency levels

/**
 * Cassandra 1.0.6 client for YCSB framework
 */
public class CassandraClient10 extends DB
{
    private static Random random = new Random();
    private static final int Ok = 0;
    private static final int Error = -1;
    private static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

    private int ConnectionRetries;
    private int OperationRetries;
    private String column_family;

    private static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
    private static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

    private static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
    private static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

    private static final String USERNAME_PROPERTY = "cassandra.username";
    private static final String PASSWORD_PROPERTY = "cassandra.password";

    private static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
    private static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    private static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
    private static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
    private static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";


    private TTransport tr;
    private Cassandra.Client client;

    private boolean _debug = false;

    private String _table = "";
    private Exception errorexception = null;

    private List<Mutation> mutations = new ArrayList<Mutation>();

    private ColumnParent parent;

    private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    private ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
    private ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
    private ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void init() throws DBException
    {
        String hosts = getProperties().getProperty("hosts");
        if (hosts == null)
        {
            throw new DBException("Required property \"hosts\" missing for CassandraClient");
        }

        column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
        parent = new ColumnParent(column_family);

        ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
                                                                         CONNECTION_RETRY_PROPERTY_DEFAULT));
        OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
                                                                        OPERATION_RETRY_PROPERTY_DEFAULT));

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));


        _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        String[] allhosts = hosts.split(",");
        String myhost = allhosts[random.nextInt(allhosts.length)];
        System.err.printf("Attempting to connect to %s:%d%n", myhost, 9160);

        Exception connectexception = null;

        for (int retry = 0; retry < ConnectionRetries; retry++)
        {
            tr = new TFramedTransport(new TSocket(myhost, 9160));
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            try
            {
                tr.open();
                connectexception = null;
                break;
            }
            catch (Exception e)
            {
                connectexception = e;
            }
            try
            {
                System.out.println("Reconnect!");
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
        }
        if (connectexception != null)
        {
            System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
                               + " tries");
            throw new DBException(connectexception);
        }

        if (username != null && password != null)
        {
            Map<String, String> cred = new HashMap<String, String>();
            cred.put("username", username);
            cred.put("password", password);
            AuthenticationRequest req = new AuthenticationRequest(cred);
            try
            {
                client.login(req);
            }
            catch (Exception e)
            {
                throw new DBException(e);
            }
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    public void cleanup() throws DBException
    {
        tr.close();
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a Map.
     *
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int readAll(String table, String key, Map<String, ByteIterator> result) {
        SliceRange range = new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(range);

        return read(table, key, result, predicate);
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int readOne(String table, String key, String field, Map<String, ByteIterator> result) {
        try
        {
            ByteBuffer fieldBuffer = ByteBuffer.wrap(field.getBytes("UTF-8"));
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(fieldBuffer));

            return read(table, key, result, predicate);
        }
        catch (Exception e)
        {
            errorexception = e;
        }

        errorexception.printStackTrace();
        return Error;
    }

    private int read(String table, String key, Map<String, ByteIterator> result, SlicePredicate predicate) {
        if (!_table.equals(table))
        {
            try
            {
                client.set_keyspace(table);
                _table = table;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++)
        {
            try
            {
                List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, readConsistencyLevel);

                if (_debug)
                {
                    System.out.print("Reading key: " + key);
                }

                Column column;
                String name;
                ByteIterator value;
                for (ColumnOrSuperColumn oneresult : results)
                {
                    column = oneresult.column;
                    name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                    value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                    result.put(name, value);

                    if (_debug)
                    {
                        System.out.print("(" + name + "=" + value + ")");
                    }
                }

                if (_debug)
                {
                    System.out.println();
                    System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
                }

                return Ok;
            }
            catch (Exception e)
            {
                errorexception = e;
            }

            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
        }
        errorexception.printStackTrace();
        return Error;

    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a Map.
     *
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param result      A List of Maps, where each Map is a set field/value
     *                    pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result)
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));
        return scan(table, startkey, recordcount, result, predicate);
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a Map.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field       The field to read
     * @param result      A List of Maps, where each Map is a set field/value pairs
     *                    for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result)
    {
        try
        {
            ByteBuffer fieldBuffer = ByteBuffer.wrap(field.getBytes("UTF-8"));
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(fieldBuffer));
            return scan(table, startkey, recordcount, result, predicate);
        }
        catch (Exception e)
        {
            errorexception = e;
        }
        errorexception.printStackTrace();
        return Error;
    }

    public int scan(String table, String startkey, int recordcount,
                    List<Map<String, ByteIterator>> result, SlicePredicate predicate) {
        if (!_table.equals(table))
        {
            try
            {
                client.set_keyspace(table);
                _table = table;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++)
        {
            try
            {
                KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[]{ }).setCount(recordcount);

                List<KeySlice> results = client.get_range_slices(parent, predicate, kr, scanConsistencyLevel);

                if (_debug)
                {
                    System.out.println("Scanning startkey: " + startkey);
                }

                Map<String, ByteIterator> tuple;
                for (KeySlice oneresult : results)
                {
                    tuple = new HashMap<String, ByteIterator>();

                    Column column;
                    String name;
                    ByteIterator value;
                    for (ColumnOrSuperColumn onecol : oneresult.columns)
                    {
                        column = onecol.column;
                        name = new String(column.name.array(), column.name.position() + column.name.arrayOffset(), column.name.remaining());
                        value = new ByteArrayByteIterator(column.value.array(), column.value.position() + column.value.arrayOffset(), column.value.remaining());

                        tuple.put(name, value);

                        if (_debug)
                        {
                            System.out.print("(" + name + "=" + value + ")");
                        }
                    }

                    result.add(tuple);
                    if (_debug)
                    {
                        System.out.println();
                        System.out.println("ConsistencyLevel=" + scanConsistencyLevel.toString());
                    }
                }

                return Ok;
            }
            catch (Exception e)
            {
                errorexception = e;
            }
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
        }
        errorexception.printStackTrace();
        return Error;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values Map will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param field  The field to be updated
     * @param value  The value to update in the key record
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
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int updateAll(String table, String key, Map<String,ByteIterator> values) {
        return insert(table, key, values);
    }

    public int update(String table, String key, Map<String, ByteIterator> values) {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values Map will be written into the record with the specified record
     * key.
     *
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A Map of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, Map<String, ByteIterator> values)
    {
        if (!_table.equals(table))
        {
            try
            {
                client.set_keyspace(table);
                _table = table;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++)
        {
            if (_debug)
            {
                System.out.println("Inserting key: " + key);
            }

            try
            {
                ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

                Column col;
                ColumnOrSuperColumn column;
                for (Map.Entry<String, ByteIterator> entry : values.entrySet())
                {
                    col = new Column();
                    col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                    col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
                    col.setTimestamp(System.currentTimeMillis());

                    column = new ColumnOrSuperColumn();
                    column.setColumn(col);

                    mutations.add(new Mutation().setColumn_or_supercolumn(column));
                }

                client.batch_mutate(Collections.singletonMap(wrappedKey,
                                                             Collections.singletonMap(column_family, mutations)),
                                    writeConsistencyLevel);

                mutations.clear();

                if (_debug)
                {
                    System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
                }

                return Ok;
            }
            catch (Exception e)
            {
                errorexception = e;
            }
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
        }

        errorexception.printStackTrace();
        return Error;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        if (!_table.equals(table))
        {
            try
            {
                client.set_keyspace(table);
                _table = table;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return Error;
            }
        }

        for (int i = 0; i < OperationRetries; i++)
        {
            try
            {
                client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
                              new ColumnPath(column_family),
                              System.currentTimeMillis(),
                              deleteConsistencyLevel);

                if (_debug)
                {
                    System.out.println("Delete key: " + key);
                    System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
                }

                return Ok;
            }
            catch (Exception e)
            {
                errorexception = e;
            }
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
        }
        errorexception.printStackTrace();
        return Error;
    }

    public static void main(String[] args)
    {
        CassandraClient10 cli = new CassandraClient10();

        Properties props = new Properties();

        props.setProperty("hosts", args[0]);
        cli.setProperties(props);

        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = cli.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = cli.readAll("usertable", "BrianFrankCooper", result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet())
        {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);
    }

    /*
    * public static void main(String[] args) throws TException,
    * InvalidRequestException, UnavailableException,
    * UnsupportedEncodingException, NotFoundException {
    *
    *
    *
    * String key_user_id = "1";
    *
    *
    *
    *
    * client.insert("Keyspace1", key_user_id, new ColumnPath("Standard1", null,
    * "age".getBytes("UTF-8")), "24".getBytes("UTF-8"), timestamp,
    * ConsistencyLevel.ONE);
    *
    *
    * // read single column ColumnPath path = new ColumnPath("Standard1", null,
    * "name".getBytes("UTF-8"));
    *
    * System.out.println(client.get("Keyspace1", key_user_id, path,
    * ConsistencyLevel.ONE));
    *
    *
    * // read entire row SlicePredicate predicate = new SlicePredicate(null, new
    * SliceRange(new byte[0], new byte[0], false, 10));
    *
    * ColumnParent parent = new ColumnParent("Standard1", null);
    *
    * List<ColumnOrSuperColumn> results = client.get_slice("Keyspace1",
    * key_user_id, parent, predicate, ConsistencyLevel.ONE);
    *
    * for (ColumnOrSuperColumn result : results) {
    *
    * Column column = result.column;
    *
    * System.out.println(new String(column.name, "UTF-8") + " -> " + new
    * String(column.value, "UTF-8"));
    *
    * }
    *
    *
    *
    *
    * }
    */
}
