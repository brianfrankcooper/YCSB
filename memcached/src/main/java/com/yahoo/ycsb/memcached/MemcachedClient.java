package com.yahoo.ycsb.memcached;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MemcachedClient extends MemcachedCompatibleClient {

    public static final String HOSTS_PROPERTY = "memcached.hosts";

    public static final int DEFAULT_PORT = 11211;

    public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "memcached.shutdownTimeoutMillis";
    public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

    public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeout";
    public static final String DEFAULT_OP_TIMEOUT = "60000";

    public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";
    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

    @Override
    protected net.spy.memcached.MemcachedClient createMemcachedClient() throws Exception {
        ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();

        connectionFactoryBuilder.setReadBufferSize(Integer.parseInt(getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS)));
        connectionFactoryBuilder.setOpTimeout(Integer.parseInt(getProperties().getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT)));
        String failureString = getProperties().getProperty(FAILURE_MODE_PROPERTY);
        connectionFactoryBuilder.setFailureMode(failureString == null ? FAILURE_MODE_PROPERTY_DEFAULT : FailureMode.valueOf(failureString));

        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String address : getProperties().getProperty(HOSTS_PROPERTY).split(",")) {
            int colon = address.indexOf(":");
            int port = DEFAULT_PORT;
            String host = address;
            if (colon != -1) {
                port = Integer.parseInt(address.substring(colon + 1));
                host = address.substring(0, colon);
            }
            addresses.add(new InetSocketAddress(host, port));
        }
        return new net.spy.memcached.MemcachedClient(connectionFactoryBuilder.build(), addresses);
    }
}
