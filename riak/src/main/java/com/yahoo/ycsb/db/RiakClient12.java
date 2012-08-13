package com.yahoo.ycsb.db;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.convert.JSONConverter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/*
  This is considered pre-alpha, gin-inspired code.
  Use at your own risk. It's currently awaiting review.
*/

public class RiakClient12 extends DB {
    IRiakClient riakClient;
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final String RIAK_CLUSTER_HOSTS = "riak_cluster_hosts";
    public static final String RIAK_CLUSTER_HOST_DEFAULT = "127.0.0.1:8087";

    public void init() throws DBException {
        try {
            Properties props = getProperties();
            String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS, RIAK_CLUSTER_HOST_DEFAULT);
            String[] servers = cluster_hosts.split(",");
            if(servers.length == 1) {
                String[] ipAndPort = servers[0].split(":");
                String ip = ipAndPort[0].trim();
                int port = Integer.parseInt(ipAndPort[1].trim());
                System.out.println("Riak connection to " + ip + ":" + port);
                riakClient = RiakFactory.pbcClient(ip, port);
            } else {
                PBClusterConfig clusterConf = new PBClusterConfig(200);
                for(String server:servers) {
                    String[] ipAndPort = server.split(":");
                    String ip = ipAndPort[0].trim();
                    int port = Integer.parseInt(ipAndPort[1].trim());
                    System.out.println("Riak connection to " + ip + ":" + port);
                    PBClientConfig node = PBClientConfig.Builder
                            .from(PBClientConfig.defaults())
                            .withHost(ip)
                            .withPort(port).build();
                    clusterConf.addClient(node);
                }
                riakClient = RiakFactory.newClient(clusterConf);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());
        }
    }

    public void cleanup() throws DBException {
        riakClient.shutdown();
    }

    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            Bucket bucket = riakClient.fetchBucket(table).execute();
            IRiakObject obj = bucket.fetch(key).execute();
            HashMap m = new JSONConverter<HashMap>(HashMap.class, table, key).toDomain(obj);
            StringByteIterator.putAllAsStrings(m, result);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // NOT implemented
        return OK;
    }

    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            Bucket bucket = riakClient.fetchBucket(table).execute();
            IRiakObject robj = bucket.fetch(key).execute();
            HashMap<String, String> m = StringByteIterator.getStringMap(values);
            @SuppressWarnings("unchecked")
            IRiakObject obj = new JSONConverter(m.getClass(), table, key).fromDomain(m,robj.getVClock());
            bucket.store(obj);
        } catch (Exception e) {
            insert(table, key, values);
        }
        return OK;
    }

    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            Bucket bucket = riakClient.fetchBucket(table).execute();
            HashMap<String, String> m = StringByteIterator.getStringMap(values);
            @SuppressWarnings("unchecked")
            IRiakObject obj = new JSONConverter(m.getClass(), table, key).fromDomain(m,null);
            bucket.store(obj);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int delete(String table, String key) {
        try {
            riakClient.fetchBucket(table).execute().delete(key);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }
}
