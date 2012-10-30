package com.yahoo.ycsb.memcached;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class MemcachedCompatibleClient extends DB {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final String QUALIFIED_KEY = "{0}-{1}";

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected MemcachedClient client;

    protected MemcachedCompatibleConfig config;

    private boolean checkOperationStatus;

    private long shutdownTimeoutMillis;

    private int objectExpirationTime;

    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;

    @Override
    public void init() throws DBException {
        try {
            config = createMemcachedConfig();
            client = createMemcachedClient();
            checkOperationStatus = config.getCheckOperationStatus();
            objectExpirationTime = config.getObjectExpirationTime();
            shutdownTimeoutMillis = config.getShutdownTimeoutMillis();
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    protected abstract MemcachedCompatibleConfig createMemcachedConfig();

    protected abstract MemcachedClient createMemcachedClient() throws Exception;

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            GetFuture<Object> future = client.asyncGet(createQualifiedKey(table, key));
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
    public int scan(String table, String startKey, int limit, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        throw new IllegalStateException("Range scan is not supported");
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.replace(key, objectExpirationTime, toJson(values));
            return getReturnCode(future);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Error updating value with key: " + key, e);
            }
            return ERROR;
        }
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.add(key, objectExpirationTime, toJson(values));
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

    public int query(String table, String key, int limit) {
        throw new UnsupportedOperationException("Query not implemented");
    };

    protected int getReturnCode(OperationFuture<Boolean> future) {
        if (checkOperationStatus) {
            return future.getStatus().isSuccess() ? OK : ERROR;
        } else {
            return OK;
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (client != null) {
            client.shutdown(shutdownTimeoutMillis, MILLISECONDS);
        }
    }

    protected static String createQualifiedKey(String table, String key) {
        return MessageFormat.format(QUALIFIED_KEY, table, key);
    }

    protected static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        JsonNode json = MAPPER.readTree(value);
        boolean checkFields = fields != null && fields.size() > 0;
        for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext(); ) {
            Map.Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (checkFields && fields.contains(name)) {
                continue;
            }
            JsonNode jsonValue = jsonField.getValue();
            if (jsonValue != null && !jsonValue.isNull()) {
                result.put(name, new StringByteIterator(jsonValue.asText()));
            }
        }
    }

    protected static String toJson(Map<String, ByteIterator> values) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        for (Map.Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
        MAPPER.writeTree(jsonGenerator, node);
        return writer.toString();
    }
}
