package com.yahoo.ycsb.db;

import static com.google.common.collect.Maps.newHashMap;
import static com.yahoo.ycsb.db.RiakUtils.*;
import static com.yahoo.ycsb.db.Constants.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public final class RiakDBClient extends DB {

    private static final AtomicLong SCAN_INDEX_SEQUENCE = new AtomicLong();

    private RiakClient riakClient;
    private RiakCluster riakCluster;
    private boolean use2i = false;
    private static int connectionNumber = 0;

    public static final int OK = 0;
    public static final int ERROR = -1;

    public void init() throws DBException {

        try {
            final Properties props = this.getProperties();
            use2i = Boolean.parseBoolean(props.getProperty(RIAK_USE_2I,
                    RIAK_USE_2I_DEFAULT));
            final String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS,
                    RIAK_CLUSTER_HOST_DEFAULT);
            final String[] servers = cluster_hosts.split(",");
            setupConnections(props, servers);
        } catch (Exception e) {

            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());

        }
    }

    private void setupConnections(Properties props, String[] servers)
            throws IOException {

        final RiakNode.Builder builder = new RiakNode.Builder();
        final List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(servers));
        riakCluster = new RiakCluster.Builder(nodes).build();
        riakCluster.start();
        riakClient = new RiakClient(riakCluster);
    }


    @Override
    public void cleanup() throws DBException {
        riakCluster.shutdown();
    }

    @Override
    public int read(final String aBucket, final String aKey,
            final Set<String> theFields, HashMap<String, ByteIterator> theResult) {

        try {

            final Namespace ns = new Namespace("default", aBucket);
            final Location location = new Location(ns, aKey);
            final FetchValue fv = new FetchValue.Builder(location).build();
            final FetchValue.Response response = riakClient.execute(fv);
            final RiakObject obj = response.getValue(RiakObject.class);
            deserializeTable(obj, theResult);

            return OK;

        } catch (Exception e) {

            e.printStackTrace();
            return ERROR;

        }
    }

    public int scan(String bucket, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return ERROR;
    }

    private class YCSBUpdate extends UpdateValue.Update<byte[]> {

        private final HashMap<String, ByteIterator> update;
        public YCSBUpdate(HashMap<String, ByteIterator> updatedColumns) {
            this.update = updatedColumns;
        }

        @Override
        public byte[] apply(byte[] original) {
            if (original == null) {
                original = new byte[0];
            }

            HashMap<String, ByteIterator> table = newHashMap();
            deserializeTable(original, table);
            Map<String, ByteIterator> updatedTable = merge(table, update);
            original = serializeTable(updatedTable);

            return original;
        }
    }

    @Override
    public int update(final String aBucket, final String aKey,
            final HashMap<String, ByteIterator> theUpdatedColumns) {

        try {

            final Namespace ns = new Namespace("default", aBucket);
            final Location location = new Location(ns, aKey);
            final YCSBUpdate update = new YCSBUpdate(theUpdatedColumns);

            final UpdateValue uv = new UpdateValue.Builder(location).withUpdate(update).build();
            final UpdateValue.Response response = riakClient.execute(uv);

            return OK;

        } catch (Exception e) {

            e.printStackTrace();
            return ERROR;

        }

    }

    @Override
    public int insert(String aBucket, String aKey,
            HashMap<String, ByteIterator> theColumns) {

        try {

            final Namespace ns = new Namespace("default", aBucket);
            final Location location = new Location(ns, aKey);
            final RiakObject object = new RiakObject();
            object.setValue(BinaryValue.create(serializeTable(theColumns)));
            StoreValue store = new StoreValue.Builder(object)
                    .withLocation(location)
                    .build();
            riakClient.execute(store);

            return OK;

        } catch (Exception e) {

            e.printStackTrace();
            return ERROR;

        }

    }

    public int delete(String bucket, String key) {
        try {
            final Namespace ns = new Namespace("default", bucket);
            final Location location = new Location(ns, key);
            final DeleteValue dv = new DeleteValue.Builder(location).build();
            riakClient.execute(dv);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public static void main(String[] args) {
        RiakDBClient cli = new RiakDBClient();

        Properties props = new Properties();
        props.setProperty(RIAK_CLUSTER_HOSTS,
                "localhost:10017, localhost:10027, localhost:10037");

        cli.setProperties(props);

        try {
            cli.init();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String bucket = "people";
        String key = "person1";

        {
            HashMap<String, String> values = new HashMap<String, String>();
            values.put("first_name", "Dave");
            values.put("last_name", "Parfitt");
            values.put("city", "Buffalo, NY");
            cli.insert(bucket, key,
                    StringByteIterator.getByteIteratorMap(values));
            System.out.println("Added person");
        }

        {
            Set<String> fields = new HashSet<String>();
            fields.add("first_name");
            fields.add("last_name");
            HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
            cli.read(bucket, key, fields, results);
            System.out.println(results.toString());
            System.out.println("Read person");
        }

        {
            HashMap<String, String> updateValues = new HashMap<String, String>();
            updateValues.put("twitter", "@metadave");
            cli.update("people", "person1",
                    StringByteIterator.getByteIteratorMap(updateValues));
            System.out.println("Updated person");
        }

        {
            HashMap<String, ByteIterator> finalResults = new HashMap<String, ByteIterator>();
            cli.read(bucket, key, null, finalResults);
            System.out.println(finalResults.toString());
            System.out.println("Read person");
        }
    }
}
