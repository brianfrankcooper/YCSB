/**
 * 
 */
package com.yahoo.ycsb.db;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Couchbase client for YCSB framework
 * 
 * <p>
 * Properties supported [represents default property value]:
 * <ul>
 * <li>hosts=COMMA_DELIMITED_HOSTS[http://127.0.0.1:8091/pools]
 * <li>table=BUCKET_NAME[default]
 * <li>password=PASSWORD[EMPTY]
 * <li>ddoc=DESIGN_DOC[null]
 * <li>view=VIEW_NAME[null]
 * <li>PersistTo={@link PersistTo} enum const name[ZERO]
 * <li>ReplicateTo=={@link ReplicateTo} enum const name[ZERO]
 * <li>Stale={@link Stale} enum const name[OK]
 * </p>
 * 
 * @author Chandan Benjaram <chandan.benjaram@gmail.com>
 * @version 0.1
 * 
 */
public class CouchbaseDbClient extends DB {

    /**
     * Couchbase client.
     */
    private volatile static CouchbaseClient client;

    /**
     * per thread {@link Gson} instance
     */
    public final ThreadLocal<Gson>          gson         = new ThreadLocal<Gson>() {
                                                             public Gson get() {
                                                                 return GSON_BUILDER
                                                                         .create();
                                                             }
                                                         };

    // shared parameters
    private volatile PersistTo              persistTo;
    private volatile ReplicateTo            replicateTo;
    private volatile Stale                  stale;
    /**
     * Count the number of times initialized to teardown on the last
     * {@link #cleanup()}.
     */
    private static final AtomicInteger      initCount    = new AtomicInteger(0);

    /**
     * {@link Gson} instance builder
     */
    private static GsonBuilder              GSON_BUILDER = new GsonBuilder()
                                                                 .registerTypeAdapter(
                                                                         ByteIterator.class,
                                                                         new JsonSerializer<ByteIterator>() {

                                                                             @Override
                                                                             public JsonElement serialize(
                                                                                     ByteIterator arg0,
                                                                                     Type arg1,
                                                                                     JsonSerializationContext arg2) {
                                                                                 return new JsonPrimitive(
                                                                                         arg0.toString());
                                                                             }
                                                                         })
                                                                 .registerTypeAdapter(
                                                                         ByteIterator.class,
                                                                         new JsonDeserializer<ByteIterator>() {
                                                                             @Override
                                                                             public ByteIterator deserialize(
                                                                                     JsonElement arg0,
                                                                                     Type arg1,
                                                                                     JsonDeserializationContext arg2)
                                                                                     throws JsonParseException {
                                                                                 return new StringByteIterator(
                                                                                         arg0.toString());
                                                                             }
                                                                         });

    /**
     * default
     */
    public CouchbaseDbClient() {
        super();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#init()
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();

        super.init();

        // singleton CouchbaseClient as it is costly to construct & thread safe
        // per docs\
        try {
            if (client == null) {
                // class level lock
                synchronized (CouchbaseDbClient.class) {
                    // double check
                    if (client != null) {
                        return;
                    }

                    // (Subset) of nodes in the cluster to establish a
                    // connection
                    final String hosts = getProperties().getProperty("hosts",
                            "http://127.0.0.1:8091/pools");
                    final List<URI> nodes = new ArrayList<URI>();
                    for (String host : hosts.split(",")) {
                        nodes.add(new URI(host));
                    }

                    // Name of the Bucket to connect to
                    final String bucket = getProperties().getProperty("table",
                            "default");

                    // bucket password
                    final String password = getProperties().getProperty(
                            "password", "");

                    // establish client connection
                    client = new CouchbaseClient(nodes, bucket, password);

                }
            }

            // defaults to zero persistence
            persistTo = PersistTo.valueOf(getProperties().getProperty(
                    "PersistTo", PersistTo.ZERO.name()).toUpperCase());
            // defaults to no replication
            replicateTo = ReplicateTo.valueOf(getProperties().getProperty(
                    "ReplicateTo", ReplicateTo.ZERO.name()).toUpperCase());
            // defaults to staled
            stale = Stale.valueOf(getProperties().getProperty("Stale",
                    Stale.OK.name()).toUpperCase());
        } catch (Exception e) {
            throw new DBException(e);
        }
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#cleanup()
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                client.shutdown();
            } catch (Exception e) {
                System.err
                        .println("Could not close Couchbase connection pool: "
                        + e.toString());
                e.printStackTrace();
                return;
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#read(java.lang.String, java.lang.String,
     * java.util.Set, java.util.HashMap)
     */
    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            Object record = client.get(key);
            ByteIterator recVal = gson.get().fromJson(record.toString(),
                    ByteIterator.class);
            result.put(key, recVal);
            return 0;
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#scan(java.lang.String, java.lang.String, int,
     * java.util.Set, java.util.Vector)
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

        String designDoc = getProperties().getProperty("ddoc");
        String viewName = getProperties().getProperty("view");

        if (designDoc == null || viewName == null) {
            System.err.println("Scan requires [ddoc, view] params");
            return 1;
        }

        try {
            final View view = client.getView(designDoc, viewName);
            Query query = new Query().setRangeStart(startkey)
                    .setLimit(recordcount).setIncludeDocs(Boolean.TRUE)
                    .setStale(stale);

            ViewResponse response = client.query(view, query);

            HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
            for (ViewRow row : response) {
                Object obj = row.getDocument();
                if (obj == null) {
                    continue;
                }
                ByteIterator recVal = gson.get().fromJson(obj.toString(),
                        ByteIterator.class);
                resultMap.put(row.getKey(), recVal);
            }
            result.add(resultMap);
            return 0;
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#update(java.lang.String, java.lang.String,
     * java.util.HashMap)
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            String encoded = gson.get().toJson(values,
                    new TypeToken<Map<String, ByteIterator>>() {
                    }.getType());
            return client.set(key, encoded, persistTo, replicateTo).get(5, TimeUnit.SECONDS)
                    .booleanValue() ? 0 : 1;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.err.println(e.getMessage());
        } catch (TimeoutException e) {
            System.err.println(e.getMessage());
        }
        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#insert(java.lang.String, java.lang.String,
     * java.util.HashMap)
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            String encoded = gson.get().toJson(values,
                    new TypeToken<Map<String, ByteIterator>>() {
                    }.getType());
            return client.add(key, encoded, persistTo, replicateTo)
                    .get(5, TimeUnit.SECONDS)
                    .booleanValue() ? 0 : 1;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ExecutionException e) {
            System.err.println(e.getMessage());
        } catch (TimeoutException e) {
            System.err.println(e.getMessage());
        }

        return 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#delete(java.lang.String, java.lang.String)
     */
    @Override
    public int delete(String table, String key) {
        try {
            return client.delete(key, persistTo, replicateTo).get(5, TimeUnit.SECONDS)
                    .booleanValue() ? 0 : 1;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ExecutionException e) {
            System.err.println(e.getMessage());
        } catch (TimeoutException e) {
            System.err.println(e.getMessage());
        }

        return 1;
    }

}
