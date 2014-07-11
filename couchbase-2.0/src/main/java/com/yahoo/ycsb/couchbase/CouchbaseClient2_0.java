package com.yahoo.ycsb.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.memcached.MemcachedCompatibleClient;
import net.spy.memcached.FailureMode;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

@SuppressWarnings({"NullableProblems"})
public class CouchbaseClient2_0 extends MemcachedCompatibleClient {
    public static final String HOSTS_PROPERTY = "couchbase.hosts";
    public static final String BUCKET_PROPERTY = "couchbase.bucket";
    public static final String DEFAULT_BUCKET = "default";
    public static final String USER_PROPERTY = "couchbase.user";
    public static final String PASSWORD_PROPERTY = "couchbase.password";
    public static final String DEFAULT_OP_TIMEOUT = "60000";
    public static final String OP_TIMEOUT_PROPERTY = "couchbase.opTimeout";
    public static final String READ_BUFFER_SIZE_PROPERTY = "couchbase.readBufferSize";
    public static final String READ_BUFFER_SIZE_DEFAULT = "16384";
    public static final String FAILURE_MODE_PROPERTY = "couchbase.failureMode";
    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;
    public static final String DDOCS_PROPERTY = "couchbase.ddocs";
    public static final String VIEWS_PROPERTY = "couchbase.views";
    public static final String PERSIST_TO_PROPERTY = "couchbase.persistTo";
    public static final PersistTo PERSIST_TO_PROPERTY_DEFAULT = PersistTo.MASTER;
    public static final String REPLICATE_TO_PROPERTY = "couchbase.replicateTo";
    public static final ReplicateTo REPLICATE_TO_PROPERTY_DEFAULT = ReplicateTo.ZERO;
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected CouchbaseClient couchbaseClient;

    private static View view;

    private String[] ddoc_names;
    private String[] view_names;
    protected PersistTo persistTo;
    protected ReplicateTo replicateTo;

    private Random generator = new Random();

    private Map<String, View> views = new HashMap<String, View>();

    @Override
    public void init() throws DBException {
        super.init();
        try {
            couchbaseClient = createCouchbaseClient();
            client = couchbaseClient;
            ddoc_names = getProperties().getProperty(DDOCS_PROPERTY).split(",");
            view_names = getProperties().getProperty(VIEWS_PROPERTY).split(",");
            String persistToValue = getProperties().getProperty(PERSIST_TO_PROPERTY);
            persistTo = persistToValue == null
                        ? PERSIST_TO_PROPERTY_DEFAULT
                        : PersistTo.valueOf(persistToValue);
            String replicateToValue = (String) getProperties().get(REPLICATE_TO_PROPERTY);
            replicateTo = replicateToValue == null
                          ? REPLICATE_TO_PROPERTY_DEFAULT
                          : ReplicateTo.valueOf(replicateToValue);
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    protected CouchbaseClient createMemcachedClient() throws Exception {
        return createCouchbaseClient();
    }

    protected CouchbaseClient createCouchbaseClient() throws Exception {
        CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
        builder.setReadBufferSize(Integer.parseInt(getProperties().getProperty(READ_BUFFER_SIZE_PROPERTY, READ_BUFFER_SIZE_DEFAULT)));
        builder.setOpTimeout(Long.parseLong(getProperties().getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT)));
        String failureModeValue = getProperties().getProperty(FAILURE_MODE_PROPERTY);
        builder.setFailureMode(failureModeValue == null
                               ? FAILURE_MODE_PROPERTY_DEFAULT
                               : FailureMode.valueOf(failureModeValue));

        List<URI> servers = new ArrayList<URI>();
        String result = getProperties().getProperty(HOSTS_PROPERTY);
        if (result == null) {
            throw new DBException("Missing required 'hosts' property");
        }
        for (String address : result.split(",")) {
            servers.add(new URI("http://" + address + ":8091/pools"));
        }
        CouchbaseConnectionFactory connectionFactory =
                builder.buildCouchbaseConnection(servers,
                                                 getProperties().getProperty(BUCKET_PROPERTY, DEFAULT_BUCKET),
                                                 getProperties().getProperty(USER_PROPERTY),
                                                 getProperties().getProperty(PASSWORD_PROPERTY));
        return new com.couchbase.client.CouchbaseClient(connectionFactory);
    }

    @Override
    public int read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            GetFuture<Object> future = couchbaseClient.asyncGet(createQualifiedKey(table, key));
            Object document = future.get();
            if (document != null) {
                fromJson((String) document, fields, result);
            }
            return OK;
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error encountered", e);
            }
            return ERROR;
        }
    }

    @Override
    public int update(String table, String key, Map<String, ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future;
            //replace(3 params) is not equal to replace(3 params, persist, replicate)
            //the second method has additional effects regardless of the passed values
            if (persistTo == null && replicateTo == null) {
                future = couchbaseClient.replace(key, objectExpirationTime, toJson(values));    //this is the method of MemcachedClient
            } else {
                future = couchbaseClient.replace(key, objectExpirationTime, toJson(values),     //this is the method of CouchbaseClient, more specific
                        persistTo, replicateTo);
            }
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error updating value with key: " + key, e);
            }
            return ERROR;
        }
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future;
            if (persistTo == null && replicateTo == null) {
                future = couchbaseClient.add(key, objectExpirationTime, toJson(values));
            } else {
                future = couchbaseClient.add(key, objectExpirationTime, toJson(values),
                    persistTo, replicateTo);
            }
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error inserting value", e);
            }
            return ERROR;
        }
    }

    @Override
    public int delete(String table, String key) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.delete(key);
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error deleting value", e);
            }
            return ERROR;
        }
    }

    @Override
    public int query(String table, String key, int limit) {
        int rnd_ddoc = generator.nextInt(ddoc_names.length);
        int rnd_view = generator.nextInt(view_names.length);
        int startIndex = 3 * rnd_ddoc + rnd_view;
        try {
            key = "field" + startIndex + key.substring(4 + startIndex, 12 + startIndex);
        } catch (StringIndexOutOfBoundsException e) {
            key = "field" + startIndex;
        }

        Query query = get_query(key, limit);
        View view = get_view(rnd_ddoc, rnd_view);
        ViewResponse response = couchbaseClient.query(view, query);

        Collection errors = response.getErrors();
        if (errors.isEmpty()) {
            return OK;
        } else {
            return ERROR;
        }
    }

    private View get_view(int rnd_ddoc, int rnd_view) {
        String ddoc_name = ddoc_names[rnd_ddoc];
        String view_name = view_names[rnd_view];
        String id = ddoc_name + view_name;

        if (views.get(id) == null) {
            view = couchbaseClient.getView(ddoc_name, view_name);
            views.put(id, view);
        }

        return view;
    }

    private Query get_query(String key, int limit) {
        Query query = new Query();
        query.setRangeStart(key);
        query.setLimit(limit);
        return query;
    }

    protected int getReturnCode(OperationFuture<Boolean> future) {
        if (checkOperationStatus) {
            return future.getStatus().isSuccess() ? OK : ERROR;
        } else {
            return OK;
        }
    }
}
