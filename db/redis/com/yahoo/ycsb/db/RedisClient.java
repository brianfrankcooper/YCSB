/**
 * Redis client binding for YCSB.
 *
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    public void cleanup() throws DBException {
        jedis.disconnect();
    }

    //XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
        if (fields == null) {
            result.putAll(jedis.hgetAll(key));
        }
        else {
            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
            List<String> values = jedis.hmget(key, fieldArray);

            Iterator<String> fieldIterator = fields.iterator();
            Iterator<String> valueIterator = values.iterator();

            while (fieldIterator.hasNext() && valueIterator.hasNext()) {
                result.put(fieldIterator.next(), valueIterator.next());
            }
            assert !fieldIterator.hasNext() && !valueIterator.hasNext();
        }
        return result.isEmpty() ? 1 : 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        return jedis.hmset(key, values).equals("OK") ? 0 : 1;
    }

    @Override
    public int delete(String table, String key) {
        return jedis.del(key) == 0 ? 1 : 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, String> values) {
        return jedis.hmset(key, values).equals("OK") ? 0 : 1;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
        //XXX
        return 0;
    }

}
