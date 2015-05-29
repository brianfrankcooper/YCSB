package com.yahoo.ycsb.memcached;

import net.spy.memcached.ConnectionFactoryBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MemcachedClient extends MemcachedCompatibleClient {

    @Override
    protected MemcachedCompatibleConfig createMemcachedConfig() {
        return new MemcachedConfig(getProperties());
    }

    @Override
    protected net.spy.memcached.MemcachedClient createMemcachedClient() throws Exception {
        ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();

        connectionFactoryBuilder.setReadBufferSize(config.getReadBufferSize());
        connectionFactoryBuilder.setOpTimeout(config.getOpTimeout());
        connectionFactoryBuilder.setFailureMode(config.getFailureMode());

        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String address : config.getHosts().split(",")) {
            int colon = address.indexOf(":");
            int port = MemcachedConfig.DEFAULT_PORT;
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
