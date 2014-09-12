package com.yahoo.ycsb.db;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Vector;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;

import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.exception.CommunicationException;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class TarantoolClient extends DB{
	DB client;

	public static final String VERSION_PROPERTY = "tnt.version";

	public void init() throws CommunicationException, DBException {
		Properties props = getProperties();
		String version = props.getProperty(VERSION_PROPERTY);
		if (version == "1.5") {
			this.client = new Tarantool15Client();
		} else {
			this.client = new Tarantool16Client();
		}
		this.client.init();
	}
	
	public void cleanup() throws DBException {
		this.client.cleanup();
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		return this.client.insert(table, key, values);
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		return this.client.read(table, key, fields, result);
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		return this.client.scan(table, startkey, recordcount, fields, result);
	}

	@Override
	public int delete(String table, String key) {
		return this.client.delete(table, key);
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return this.client.update(table, key, values);
	}
}
