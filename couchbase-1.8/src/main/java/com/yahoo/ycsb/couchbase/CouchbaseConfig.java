package com.yahoo.ycsb.couchbase;

import com.yahoo.ycsb.memcached.MemcachedCompatibleConfig;
import com.yahoo.ycsb.config.PropertiesConfig;
import net.spy.memcached.FailureMode;

import java.util.Properties;

public class CouchbaseConfig extends PropertiesConfig implements MemcachedCompatibleConfig {

    public static final String HOSTS_PROPERTY = "couchbase.hosts";

    public static final String BUCKET_PROPERTY = "couchbase.bucket";

    public static final String DEFAULT_BUCKET = "default";

    public static final String USER_PROPERTY = "couchbase.user";

    public static final String PASSWORD_PROPERTY = "couchbase.password";

    public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "couchbase.shutdownTimeoutMillis";

    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30000;

    public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "couchbase.objectExpirationTime";

    public static final int DEFAULT_OBJECT_EXPIRATION_TIME = Integer.MAX_VALUE;

    public static final String CHECK_OPERATION_STATUS_PROPERTY = "couchbase.checkOperationStatus";

    public static final boolean CHECK_OPERATION_STATUS_DEFAULT = true;

    public static final long DEFAULT_OP_TIMEOUT = 60000;

    public static final String OP_TIMEOUT_PROPERTY = "couchbase.opTimeout";

    public static final String READ_BUFFER_SIZE_PROPERTY = "couchbase.readBufferSize";

    public static final int READ_BUFFER_SIZE_DEFAULT = 16384;

    public static final String FAILURE_MODE_PROPERTY = "couchbase.failureMode";

    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

    public CouchbaseConfig(Properties properties) {
        super(properties);
        declareProperty(HOSTS_PROPERTY, true);
        declareProperty(BUCKET_PROPERTY, DEFAULT_BUCKET);
        declareProperty(USER_PROPERTY, false);
        declareProperty(PASSWORD_PROPERTY, false);
        declareProperty(CHECK_OPERATION_STATUS_PROPERTY, CHECK_OPERATION_STATUS_DEFAULT);
        declareProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT);
        declareProperty(READ_BUFFER_SIZE_PROPERTY, READ_BUFFER_SIZE_DEFAULT);
        declareProperty(FAILURE_MODE_PROPERTY, FAILURE_MODE_PROPERTY_DEFAULT);
        declareProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
        declareProperty(OBJECT_EXPIRATION_TIME_PROPERTY, DEFAULT_OBJECT_EXPIRATION_TIME);
    }

    @Override
    public String getHosts() {
        return getString(HOSTS_PROPERTY);
    }

    public String getBucket() {
        return getString(BUCKET_PROPERTY);
    }

    public String getUser() {
        return getString(USER_PROPERTY);
    }

    public String getPassword() {
        return getString(PASSWORD_PROPERTY);
    }

    @Override
    public boolean getCheckOperationStatus() {
        return getBoolean(CHECK_OPERATION_STATUS_PROPERTY);
    }

    @Override
    public long getOpTimeout() {
        return getLong(OP_TIMEOUT_PROPERTY);
    }

    @Override
    public int getReadBufferSize() {
        return getInteger(READ_BUFFER_SIZE_PROPERTY);
    }

    @Override
    public FailureMode getFailureMode() {
        String failureModeValue = getProperty(FAILURE_MODE_PROPERTY);
        return failureModeValue != null ?
                FailureMode.valueOf(failureModeValue) :
                this.<FailureMode>getDefaultValue(FAILURE_MODE_PROPERTY);
    }

    @Override
    public long getShutdownTimeoutMillis() {
        return getLong(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY);
    }

    @Override
    public int getObjectExpirationTime() {
        return getInteger(OBJECT_EXPIRATION_TIME_PROPERTY);
    }
}