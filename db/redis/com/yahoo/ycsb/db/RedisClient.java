/**
 * Redis client binding for YCSB.
 *
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisClient extends DB {

    private Jedis jedis;

    public static final String HOST_PROPERTY = "redis.host";
    public static final String PORT_PROPERTY = "redis.port";
    public static final String PASSWORD_PROPERTY = "redis.password";

    public void init() throws DBException {
        Properties props = getProperties();
        int port;

        String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null) {
            port = Integer.parseInt(portString);
        }
        else {
            port = Protocol.DEFAULT_PORT;
        }
        String host = props.getProperty(HOST_PROPERTY);

        jedis = new Jedis(host, port);
        jedis.connect();

        String password = props.getProperty(PASSWORD_PROPERTY);
        if (password != null) {
            jedis.auth(password);
            jedis.flushAll();
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, String> values) {
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
        return 0;
    }

}
