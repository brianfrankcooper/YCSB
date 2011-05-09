/**
 * Redis client binding for YCSB.
 *
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class RedisClient extends DB {
    public void init() throws DBException {
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
    }

    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
    }

    @Override
    public int delete(String table, String key) {
    }

    @Override
    public int update(String table, String key, HashMap<String, String> values) {
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
    }

}
