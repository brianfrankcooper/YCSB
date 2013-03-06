package com.yahoo.ycsb.db;


import java.nio.charset.Charset;

public class Constants {
    public static final String RIAK_SERIALIZER = "riak_serializer";
    public static final String RIAK_POOL_ENABLED = "riak_pool_enabled";
    public static final String RIAK_POOL_TOTAL_MAX_CONNECTION = "riak_pool_total_max_connection";
    public static final String RIAK_POOL_IDLE_CONNECTION_TTL_MILLIS = "riak_pool_idle_connection_ttl_millis";
    public static final String RIAK_POOL_INITIAL_POOL_SIZE = "riak_pool_initial_pool_size";
    public static final String RIAK_POOL_REQUEST_TIMEOUT_MILLIS = "riak_pool_request_timeout_millis";
    public static final String RIAK_POOL_CONNECTION_TIMEOUT_MILLIS = "riak_pool_connection_timeout_millis";
    public static final String RIAK_USE_2I = "riak_use_2i";
    public static final String YCSB_INT = "ycsb_int";
    public static final String RIAK_CLUSTER_HOSTS = "riak_cluster_hosts";
    public static final String RIAK_CLUSTER_HOST_DEFAULT = "127.0.0.1:10017";

    public static final int RIAK_POOL_TOTAL_MAX_CONNECTIONS_DEFAULT = 50;
    public static final int RIAK_POOL_IDLE_CONNETION_TTL_MILLIS_DEFAULT = 1000;
    public static final int RIAK_POOL_INITIAL_POOL_SIZE_DEFAULT = 5;
    public static final int RIAK_POOL_REQUEST_TIMEOUT_MILLIS_DEFAULT = 1000;
    public static final int RIAK_POOL_CONNECTION_TIMEOUT_MILLIS_DEFAULT = 1000;
    public static final String RIAK_USE_2I_DEFAULT = "false";

    public static final String UTF_8 = "UTF-8";
    public static final Charset CHARSET_UTF8 = Charset.forName(UTF_8);
    public static final String CONTENT_TYPE_JSON_UTF8 = "application/json;charset=UTF-8";

    public static final String RIAK_DEFAULT_SERIALIZER = "com.yahoo.ycsb.db.RiakJsonSerializer";

}
