package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.RequestLimitConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * A database interface layer for Oracle NoSQL Database.
 */
public class NoSqlDbClient extends DB {
	
	public static final int OK = 0;
	public static final int ERROR = -1;
	
	KVStore store;
	
	private int getPropertyInt(Properties properties, String key, int defaultValue) throws DBException {
		String p = properties.getProperty(key);
		int i = defaultValue;
		if (p != null) {
			try {
				i = Integer.parseInt(p);
			} catch (NumberFormatException e) {
				throw new DBException("Illegal number format in " + key + " property");
			}
		}
		return i;
	}

	@Override
	public void init() throws DBException {
		Properties properties = getProperties();
		
		/* Mandatory properties */
		String storeName = properties.getProperty("storeName", "kvstore");
		String[] helperHosts = properties.getProperty("helperHost", "localhost:5000").split(",");
		
		KVStoreConfig config = new KVStoreConfig(storeName, helperHosts);
		
		/* Optional properties */
		String p;
		
		p = properties.getProperty("consistency");
		if (p != null) {
			if (p.equalsIgnoreCase("ABSOLUTE")) {
				config.setConsistency(Consistency.ABSOLUTE);
			} else if (p.equalsIgnoreCase("NONE_REQUIRED")) {
				config.setConsistency(Consistency.NONE_REQUIRED);
			} else {
				throw new DBException("Illegal value in consistency property");
			}
		}
		
		p = properties.getProperty("durability");
		if (p != null) {
			if (p.equalsIgnoreCase("COMMIT_NO_SYNC")) {
				config.setDurability(Durability.COMMIT_NO_SYNC);
			} else if (p.equalsIgnoreCase("COMMIT_SYNC")) {
				config.setDurability(Durability.COMMIT_SYNC);
			} else if (p.equalsIgnoreCase("COMMIT_WRITE_NO_SYNC")) {
				config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
			} else {
				throw new DBException("Illegal value in durability property");
			}
		}
		
		int maxActiveRequests = getPropertyInt(properties,
				"requestLimit.maxActiveRequests", RequestLimitConfig.DEFAULT_MAX_ACTIVE_REQUESTS);
		int requestThresholdPercent = getPropertyInt(properties,
				"requestLimit.requestThresholdPercent", RequestLimitConfig.DEFAULT_REQUEST_THRESHOLD_PERCENT);
		int nodeLimitPercent = getPropertyInt(properties,
				"requestLimit.nodeLimitPercent", RequestLimitConfig.DEFAULT_NODE_LIMIT_PERCENT);
		RequestLimitConfig requestLimitConfig;
		/* It is said that the constructor could throw NodeRequestLimitException in Javadoc, the exception is not provided */
//		try {
			requestLimitConfig = new RequestLimitConfig(maxActiveRequests, requestThresholdPercent, nodeLimitPercent);
//		} catch (NodeRequestLimitException e) {
//			throw new DBException(e);
//		}
		config.setRequestLimit(requestLimitConfig);

		p = properties.getProperty("requestTimeout");
		if (p != null) {
			long timeout = 1;
			try {
				timeout = Long.parseLong(p); 
			} catch (NumberFormatException e) {
				throw new DBException("Illegal number format in requestTimeout property");
			}
			try {
				// TODO Support other TimeUnit
				config.setRequestTimeout(timeout, TimeUnit.SECONDS);
			} catch (IllegalArgumentException e) {
				throw new DBException(e);
			}
		}
		
		try {
			store = KVStoreFactory.getStore(config);
		} catch (FaultException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void cleanup() throws DBException {
		store.close();
	}
	
	/**
	 * Create a key object.
	 * We map "table" and (YCSB's) "key" to a major component of the oracle.kv.Key,
	 * and "field" to a minor component.
	 * 
	 * @return An oracle.kv.Key object.
	 */
	private static Key createKey(String table, String key, String field) {
		List<String> majorPath = new ArrayList<String>();
		majorPath.add(table);
		majorPath.add(key);
		if (field == null) {
			return Key.createKey(majorPath);
		}
		
		return Key.createKey(majorPath, field);
	}
	
	private static Key createKey(String table, String key) {
		return createKey(table, key, null);
	}
	
	private static String getFieldFromKey(Key key) {
		return key.getMinorPath().get(0);
	}

	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		Key kvKey = createKey(table, key);
		SortedMap<Key, ValueVersion> kvResult;
		try {
			kvResult = store.multiGet(kvKey, null, null);
		} catch (FaultException e) {
			System.err.println(e);
			return ERROR;
		}
		
		for (Map.Entry<Key, ValueVersion> entry : kvResult.entrySet()) {
			/* If fields is null, read all fields */
			String field = getFieldFromKey(entry.getKey());
			if (fields != null && !fields.contains(field)) {
				continue;
			}
			result.put(field, new ByteArrayByteIterator(entry.getValue().getValue().getValue()));
		}
		
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		System.err.println("Oracle NoSQL Database does not support Scan semantics");
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			Key kvKey = createKey(table, key, entry.getKey());
			Value kvValue = Value.createValue(entry.getValue().toArray());
			try {
				store.put(kvKey, kvValue);
			} catch (FaultException e) {
				System.err.println(e);
				return ERROR;
			}
		}
		
		return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		return update(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		Key kvKey = createKey(table, key);
		try {
			store.multiDelete(kvKey, null, null);
		} catch (FaultException e) {
			System.err.println(e);
			return ERROR;
		}
		
		return OK;
	}

}
