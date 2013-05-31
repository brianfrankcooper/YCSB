package com.yahoo.ycsb.db;

import static com.google.common.collect.Maps.newHashMap;
import static com.yahoo.ycsb.db.RiakUtils.deserializeTable;
import static com.yahoo.ycsb.db.RiakUtils.serializeTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.pbc.RiakClient;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class RiakClient13 extends DB {

    public static final String RIAK_POOL_ENABLED = "riak_pool_enabled";
    public static final String RIAK_POOL_TOTAL_MAX_CONNECTION = "riak_pool_total_max_connection";
    public static final String RIAK_POOL_IDLE_CONNECTION_TTL_MILLIS = "riak_pool_idle_connection_ttl_millis";
    public static final String RIAK_POOL_INITIAL_POOL_SIZE = "riak_pool_initial_pool_size";
    public static final String RIAK_POOL_REQUEST_TIMEOUT_MILLIS = "riak_pool_request_timeout_millis";
    public static final String RIAK_POOL_CONNECTION_TIMEOUT_MILLIS = "riak_pool_connection_timeout_millis";
    private RawClient rawClient;
    public static final int OK = 0;
    public static final int ERROR = -1;

    public static final String RIAK_CLUSTER_HOSTS = "riak_cluster_hosts";
    public static final String RIAK_CLUSTER_HOST_DEFAULT = "127.0.0.1:10017";

    public static final int RIAK_POOL_TOTAL_MAX_CONNECTIONS_DEFAULT = 50;
    public static final int RIAK_POOL_IDLE_CONNETION_TTL_MILLIS_DEFAULT = 1000;
    public static final int RIAK_POOL_INITIAL_POOL_SIZE_DEFAULT = 5;
    public static final int RIAK_POOL_REQUEST_TIMEOUT_MILLIS_DEFAULT = 1000;
    public static final int RIAK_POOL_CONNECTION_TIMEOUT_MILLIS_DEFAULT = 1000;

    private static int connectionNumber = 0;

    private int getIntProperty(Properties props, String propname,
            int defaultValue) {
        String stringProp = props.getProperty(propname, "" + defaultValue);
        return Integer.parseInt(stringProp);
    }

    @Override
    public void init() throws DBException {
        try {
            Properties props = getProperties();
            String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS,
                    RIAK_CLUSTER_HOST_DEFAULT);
            String[] servers = cluster_hosts.split(",");
            boolean useConnectionPool = true;
            if (props.containsKey(RIAK_POOL_ENABLED)) {
                String usePool = props.getProperty(RIAK_POOL_ENABLED);
                useConnectionPool = Boolean.parseBoolean(usePool);
            }
            if (useConnectionPool) {
                setupConnectionPool(props, servers);
            } else {
                setupConnections(props, servers);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());
        }
    }

    private void setupConnections(Properties props, String[] servers)
            throws IOException {
        String server = servers[connectionNumber++ % servers.length];
        String[] ipAndPort = server.split(":");
        String ip = ipAndPort[0].trim();
        int port = Integer.parseInt(ipAndPort[1].trim());
        // System.out.println("Riak connection to " + ip + ":" + port);
        RiakClient pbcClient = new RiakClient(ip, port);
        rawClient = new PBClientAdapter(pbcClient);
    }

    private void setupConnectionPool(Properties props, String[] servers)
            throws IOException {

        int poolMaxConnections = getIntProperty(props,
                RIAK_POOL_TOTAL_MAX_CONNECTION,
                RIAK_POOL_TOTAL_MAX_CONNECTIONS_DEFAULT);
        int poolIdleConnectionTtlMillis = getIntProperty(props,
                RIAK_POOL_IDLE_CONNECTION_TTL_MILLIS,
                RIAK_POOL_IDLE_CONNETION_TTL_MILLIS_DEFAULT);
        int poolInitialPoolSize = getIntProperty(props,
                RIAK_POOL_INITIAL_POOL_SIZE,
                RIAK_POOL_INITIAL_POOL_SIZE_DEFAULT);
        int poolRequestTimeoutMillis = getIntProperty(props,
                RIAK_POOL_REQUEST_TIMEOUT_MILLIS,
                RIAK_POOL_REQUEST_TIMEOUT_MILLIS_DEFAULT);
        int poolConnectionTimeoutMillis = getIntProperty(props,
                RIAK_POOL_CONNECTION_TIMEOUT_MILLIS,
                RIAK_POOL_CONNECTION_TIMEOUT_MILLIS_DEFAULT);

        PBClusterConfig clusterConf = new PBClusterConfig(poolMaxConnections);
        for (String server : servers) {
            String[] ipAndPort = server.split(":");
            String ip = ipAndPort[0].trim();
            int port = Integer.parseInt(ipAndPort[1].trim());
            // System.out.println("Riak connection to " + ip + ":" + port);
            PBClientConfig node = PBClientConfig.Builder
                    .from(PBClientConfig.defaults()).withHost(ip)
                    .withPort(port)
                    .withIdleConnectionTTLMillis(poolIdleConnectionTtlMillis)
                    .withInitialPoolSize(poolInitialPoolSize)
                    .withRequestTimeoutMillis(poolRequestTimeoutMillis)
                    .withConnectionTimeoutMillis(poolConnectionTimeoutMillis)
                    .build();
            clusterConf.addClient(node);
        }
        rawClient = PBClusterClientFactory.getInstance().newClient(clusterConf);
    }

    @Override
    public void cleanup() throws DBException {
        rawClient.shutdown();
    }

    @Override
    public int read(final String aBucket, final String aKey,
            final Set<String> theFields, HashMap<String, ByteIterator> theResult) {

        try {

            final RiakResponse aResponse = rawClient.fetch(aBucket, aKey);
            if (aResponse.hasValue()) {
                final IRiakObject aRiakObject = aResponse.getRiakObjects()[0];
                deserializeTable(aRiakObject.getValue(), theResult);
            }

            return OK;

        } catch (Exception e) {

            e.printStackTrace();
            return ERROR;

        }
    }

    @Override
    public int scan(String bucket, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return OK;
    }

    @Override
    public int update(final String aBucket, final String aKey,
            final HashMap<String, ByteIterator> theUpdatedColumns) {

        try {

            final RiakResponse aResponse = rawClient.fetch(aBucket, aKey);
            if (aResponse.hasValue()) {

                final IRiakObject aRiakObject = aResponse.getRiakObjects()[0];

                final HashMap<String, ByteIterator> aTable = newHashMap();
                deserializeTable(aRiakObject.getValue(), aTable);

                for (Map.Entry<String, ByteIterator> aColumn : theUpdatedColumns
                        .entrySet()) {
                    aTable.put(aColumn.getKey(), aColumn.getValue());
                }

                final RiakObjectBuilder builder = RiakObjectBuilder
                        .newBuilder(aBucket, aKey)
                        .withValue(serializeTable(aTable))
                        .withVClock(aResponse.getVclock());
                rawClient.store(builder.build());

            }

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

            final RiakObjectBuilder aBuilder = RiakObjectBuilder.newBuilder(
                    aBucket, aKey);

            rawClient.store(aBuilder.withValue(serializeTable(theColumns))
                    .build());
            return OK;

        } catch (Exception e) {

            e.printStackTrace();
            return ERROR;

        }

    }

    public int delete(String bucket, String key) {
        try {
            rawClient.delete(bucket, key);
        } catch (IOException e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public static void main(String[] args) {
        RiakClient13 cli = new RiakClient13();

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
