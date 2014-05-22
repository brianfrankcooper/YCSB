package com.yahoo.ycsb.memcached;

import com.yahoo.ycsb.config.PropertiesConfig;
import net.spy.memcached.FailureMode;
import net.spy.memcached.ReplicateTo;

import java.util.Properties;

public class MemcachedConfig extends PropertiesConfig implements MemcachedCompatibleConfig {

    public static final String HOSTS_PROPERTY = "memcached.hosts";

    public static final int DEFAULT_PORT = 11211;

    public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "memcached.shutdownTimeoutMillis";

    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30000;

    public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "memcached.objectExpirationTime";

    public static final int DEFAULT_OBJECT_EXPIRATION_TIME = Integer.MAX_VALUE;

    public static final String CHECK_OPERATION_STATUS_PROPERTY = "memcached.checkOperationStatus";

    public static final boolean CHECK_OPERATION_STATUS_DEFAULT = true;

    public static final long DEFAULT_OP_TIMEOUT = 60000;

    public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeout";

    public static final String READ_BUFFER_SIZE_PROPERTY = "memcached.readBufferSize";

    public static final int READ_BUFFER_SIZE_DEFAULT = 16384;

    public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";

    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

    public MemcachedConfig(Properties properties) {
        super(properties);
        declareProperty(HOSTS_PROPERTY, true);
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
