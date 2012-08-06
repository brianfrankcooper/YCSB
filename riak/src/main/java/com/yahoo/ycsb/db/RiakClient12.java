package com.yahoo.ycsb.db;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

/*
  This is considered pre-alpha, gin-inspired code. 
  Use at your own risk. It's currently awaiting review.
*/

public class RiakClient12 extends DB {
    IRiakClient riakClient;

    public static final int OK = 0;
    public static final int ERROR = -1;
    //public static final int NOT_FOUND = -2;
    public static final String RIAK_HOST = "riak_host";
    public static final String RIAK_HOST_DEFAULT = "127.0.0.1";

    public static final String RIAK_PORT = "riak_port";
    public static final String RIAK_PORT_DEFAULT = "8087";

    public void init() throws DBException {
        try {
            Properties props = getProperties();
            // should probably expand this to allow more than 1 host etc
            String ip = props.getProperty(RIAK_HOST, RIAK_HOST_DEFAULT);
            int port = Integer.parseInt(props.getProperty(RIAK_PORT, RIAK_PORT_DEFAULT));
            riakClient = RiakFactory.pbcClient(ip, port);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
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
            ByteArrayInputStream bis = new ByteArrayInputStream(obj.getValue());
            ObjectInputStream ois = new ObjectInputStream(bis);
            // this might be really slow :-(
            HashMap<String, String> kv = (HashMap<String, String>) ois.readObject();
            StringByteIterator.putAllAsStrings(kv, result);
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
        insert(table, key, values);
        return OK;
    }

    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            // can I just call fetchBucket()?
            Bucket bucket = riakClient.createBucket(table).execute();
            HashMap<String, String> m = StringByteIterator.getStringMap(values);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            // this might be really slow :-(
            oos.writeObject(m);
            bucket.store(key, bos.toByteArray()).execute();
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

    public static void main(String[] args) {
        RiakClient12 client = new RiakClient12();
        Properties props = new Properties();
        try {
            //client.setProperties(props);
            client.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = client.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = client.read("usertable", "BrianFrankCooper", null, result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet()) {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = client.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);
    }
}
