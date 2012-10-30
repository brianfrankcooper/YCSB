package com.yahoo.ycsb.memcached;

import net.spy.memcached.FailureMode;

public interface MemcachedCompatibleConfig {

    String getHosts();

    boolean getCheckOperationStatus();

    long getOpTimeout();

    int getReadBufferSize();

    FailureMode getFailureMode();

    long getShutdownTimeoutMillis();

    int getObjectExpirationTime();
}