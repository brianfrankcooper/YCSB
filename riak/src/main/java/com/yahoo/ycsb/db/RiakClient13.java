package com.yahoo.ycsb.db;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IntRangeQuery;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.pbc.RiakClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.serializers.RiakSerializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static com.yahoo.ycsb.db.Constants.*;

public class RiakClient13 extends DB {
    private RawClient rawClient;
    private RiakSerializer serializer = null;
    private boolean use2i = false;
    private static int connectionNumber = 0;

    public static final int OK = 0;
    public static final int ERROR = -1;

    Map<String, Long> bucketIndexes = new HashMap<String, Long>();

    private int getIntProperty(Properties props, String propname, int defaultValue) {
        String stringProp = props.getProperty(propname, "" + defaultValue);
        return Integer.parseInt(stringProp);
    }

    public void init() throws DBException {
        try {
            Properties props = getProperties();
            use2i = Boolean.parseBoolean(props.getProperty(RIAK_USE_2I, RIAK_USE_2I_DEFAULT));
            String serializerName = props.getProperty(RIAK_SERIALIZER, RIAK_DEFAULT_SERIALIZER);
            serializer = (RiakSerializer)Class.forName(serializerName).newInstance();
            String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS, RIAK_CLUSTER_HOST_DEFAULT);
            String[] servers = cluster_hosts.split(",");
            boolean useConnectionPool = true;
            if(props.containsKey(RIAK_POOL_ENABLED)) {
                String usePool = props.getProperty(RIAK_POOL_ENABLED);
                useConnectionPool = Boolean.parseBoolean(usePool);
            }
            if(useConnectionPool) {
                setupConnectionPool(props, servers);
            }  else {
                setupConnections(props, servers);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());
        }
    }

    private void setupConnections(Properties props, String[] servers) throws IOException {
        String server = servers[connectionNumber++ % servers.length];
        String[] ipAndPort = server.split(":");
        String ip = ipAndPort[0].trim();
        int port = Integer.parseInt(ipAndPort[1].trim());
        //System.out.println("Riak connection to " + ip + ":" + port);
        RiakClient pbcClient = new RiakClient(ip, port);
        rawClient = new PBClientAdapter(pbcClient);
    }

    private void setupConnectionPool(Properties props, String[] servers) throws IOException {

        int poolMaxConnections =
                getIntProperty(props,
                        RIAK_POOL_TOTAL_MAX_CONNECTION,
                        RIAK_POOL_TOTAL_MAX_CONNECTIONS_DEFAULT);
        int poolIdleConnectionTtlMillis =
                getIntProperty(props,
                        RIAK_POOL_IDLE_CONNECTION_TTL_MILLIS,
                        RIAK_POOL_IDLE_CONNETION_TTL_MILLIS_DEFAULT);
        int poolInitialPoolSize =
                getIntProperty(props,
                        RIAK_POOL_INITIAL_POOL_SIZE,
                        RIAK_POOL_INITIAL_POOL_SIZE_DEFAULT);
        int poolRequestTimeoutMillis =
                getIntProperty(props,
                        RIAK_POOL_REQUEST_TIMEOUT_MILLIS,
                        RIAK_POOL_REQUEST_TIMEOUT_MILLIS_DEFAULT);
        int poolConnectionTimeoutMillis =
                getIntProperty(props,
                        RIAK_POOL_CONNECTION_TIMEOUT_MILLIS,
                        RIAK_POOL_CONNECTION_TIMEOUT_MILLIS_DEFAULT);

        PBClusterConfig clusterConf = new PBClusterConfig(poolMaxConnections);
        for(String server:servers) {
            String[] ipAndPort = server.split(":");
            String ip = ipAndPort[0].trim();
            int port = Integer.parseInt(ipAndPort[1].trim());
            //System.out.println("Riak connection to " + ip + ":" + port);
            PBClientConfig node = PBClientConfig.Builder
                    .from(PBClientConfig.defaults())
                    .withHost(ip)
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

    public void cleanup() throws DBException {
        rawClient.shutdown();
    }

    public int read(String bucket, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            RiakResponse response = rawClient.fetch(bucket, key);
            if(response.hasValue()) {
                // TODO: conflict resolver
                IRiakObject obj = response.getRiakObjects()[0];
                serializer.documentFromRiak(obj, fields, result);
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int scan(String bucket, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        if(use2i) {
            try {
                RiakResponse fetchResp = rawClient.fetch(bucket, startkey);
                Set<Long> idx = fetchResp.getRiakObjects()[0].getIntIndexV2(YCSB_INT);
                if(idx.size() == 0) {
                    System.err.println("Index not found");
                    return ERROR;
                } else {
                    Long id = idx.iterator().next();
                    long range = id + recordcount;
                    IndexQuery iq = new IntRangeQuery(IntIndex.named(YCSB_INT), bucket, id, range);
                    List<String> results = rawClient.fetchIndex(iq);
                    for(String key: results) {
                        RiakResponse resp = rawClient.fetch(bucket, key);
                        HashMap<String, ByteIterator> rowResult = new HashMap<String, ByteIterator>();
                                serializer.rowFromRiakScan(resp.getRiakObjects()[0], fields, rowResult);
                        result.add(rowResult);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return ERROR;
            }
            return OK;
        } else {
            return ERROR;
        }
    }

    public int update(String bucket, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            RiakResponse response = rawClient.fetch(bucket, key);
            if(response.hasValue()) {
                // TODO: conflict resolver
                IRiakObject obj = response.getRiakObjects()[0];
                byte[] data = serializer.updateDocumentFromRiak(obj, values);
                RiakObjectBuilder builder =
                        RiakObjectBuilder.newBuilder(bucket, key)
                                .withContentType(CONTENT_TYPE_JSON_UTF8)
                                .withValue(data)
                                .withVClock(response.getVclock());
                rawClient.store(builder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int insert(String bucket, String key,
                      HashMap<String, ByteIterator> values) {
        RiakObjectBuilder builder =
                RiakObjectBuilder.newBuilder(bucket, key)
                                 .withContentType(CONTENT_TYPE_JSON_UTF8);
        try {
            byte[] rawValue = serializer.documentToRiak(values);
            RiakObjectBuilder objBuilder = builder.withValue(rawValue);
            if(use2i) {
                    long idxValue = 0;
                    if(!bucketIndexes.containsKey(bucket)) {
                        bucketIndexes.put(bucket, (long)0);
                    } else {
                        idxValue = bucketIndexes.get(bucket);
                        bucketIndexes.put(bucket, idxValue+1);
                    }
                    objBuilder.addIndex(YCSB_INT, idxValue).build();
            }
            rawClient.store(objBuilder.build());
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }

        return OK;
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



    public static void main(String[] args)
    {
        RiakClient13 cli = new RiakClient13();

        Properties props = new Properties();
        props.setProperty(RIAK_CLUSTER_HOSTS, "localhost:10017, localhost:10027, localhost:10037");

        cli.setProperties(props);

        try {
            cli.init();
        } catch(Exception e) {
            e.printStackTrace();
        }

        String bucket = "people";
        String key = "person1";

        {
            HashMap<String, String> values = new HashMap<String, String>();
            values.put("first_name", "Dave");
            values.put("last_name", "Parfitt");
            values.put("city", "Buffalo, NY");
            cli.insert(bucket, key, StringByteIterator.getByteIteratorMap(values));
            System.out.println("Added person");
        }

        {
            Set<String> fields = new HashSet<String>();
            fields.add("first_name");
            fields.add("last_name");
            HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
            cli.read(bucket, key,fields, results);
            System.out.println(results.toString());
            System.out.println("Read person");
        }

        {
            HashMap<String, String> updateValues = new HashMap<String, String>();
            updateValues.put("twitter", "@metadave");
            cli.update("people", "person1", StringByteIterator.getByteIteratorMap(updateValues));
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
