/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient extends DB {

    private ShardedJedisPool pool;

    public static final String HOST_PROPERTY = "redis.host";
    public static final String PORT_PROPERTY = "redis.port";
    public static final String PASSWORD_PROPERTY = "redis.password";

    public static final String INDEX_KEY = "_indices";

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
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        String[] hosts = host.split(",");

        for (String s : hosts) {
            String parts[] = s.split(":");
            if (parts.length > 1) {
               host = parts[0];
               port = Integer.parseInt(parts[1]);
            }
            JedisShardInfo si = new JedisShardInfo(host, port);
            shards.add(si);
        }
        pool = new ShardedJedisPool(new Config(), shards);

    }

    public void cleanup() throws DBException {
        pool.destroy();
    }

    /* Calculate a hash for a key to store it in an index.  The actual return
     * value of this function is not interesting -- it primarily needs to be
     * fast and scattered along the whole space of doubles.  In a real world
     * scenario one would probably use the ASCII values of the keys.
     */
    private double hash(String key) {
        return key.hashCode();
    }

    //XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        ShardedJedis jedis = pool.getResource();
        try {
            if (fields == null) {
                StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
            }
            else {
                String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
                List<String> values = jedis.hmget(key, fieldArray);

                Iterator<String> fieldIterator = fields.iterator();
                Iterator<String> valueIterator = values.iterator();

                while (fieldIterator.hasNext() && valueIterator.hasNext()) {
                    result.put(fieldIterator.next(),
                               new StringByteIterator(valueIterator.next()));
                }
                assert !fieldIterator.hasNext() && !valueIterator.hasNext();
            }
        }
        catch (JedisConnectionException e) {
            pool.returnResource(jedis);
            return 1;
        }
        pool.returnResource(jedis);
        return result.isEmpty() ? 1 : 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        ShardedJedis jedis = pool.getResource();
        try {
            if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
                jedis.zadd(INDEX_KEY, hash(key), key);
                pool.returnResource(jedis);
                return 0;
            }
            pool.returnResource(jedis);
            return 1;
        }
        catch (JedisConnectionException e) {
            pool.returnResource(jedis);
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        ShardedJedis jedis = pool.getResource();
        try {
            int r = jedis.del(key) == 0
              && jedis.zrem(INDEX_KEY, key) == 0
                ? 1 : 0;
            pool.returnResource(jedis);
            return r;
        }
        catch (JedisConnectionException e) {
            pool.returnResource(jedis);
            return 1;
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        ShardedJedis jedis = pool.getResource();
        try {
            int r = jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
            pool.returnResource(jedis);
            return r;
        }
        catch (JedisConnectionException e) {
            pool.returnResource(jedis);
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        ShardedJedis jedis = pool.getResource();
        try {
            Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
                                Double.POSITIVE_INFINITY, 0, recordcount);
            pool.returnResource(jedis);
            HashMap<String, ByteIterator> values;
            for (String key : keys) {
                values = new HashMap<String, ByteIterator>();
                read(table, key, fields, values);
                result.add(values);
            }
        }
        catch (JedisConnectionException e) {
            pool.returnResource(jedis);
            return 1;
        }

        return 0;
    }

}
