package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public abstract class CacheClient extends DB{

	public static final String CLEAR_BEFORE_PROP = "clearbeforetest";

	public static final String PAYLOAD_TYPE_PROP = "payloadtype";

	public static final String PAYLOAD_CLASS_TYPE_PROP = "payloadclasstype";

	public static final String PAYLOAD_TYPES[] = { "regular", "indexed" };

	public static boolean regularPayloadMode = true;

	public static Class dataClass = Data.class;
	public static String payloadClassType = null;

	public static boolean batchMode = false;
	public static int batchSize = 0;
	public static AtomicInteger counter = new AtomicInteger(0);

	public Map<String, byte[]> convertToBytearrayMap(
			Map<String, ByteIterator> values) {
		Map<String, byte[]> retVal = new HashMap<String, byte[]>();
		for (String key : values.keySet()) {
			retVal.put(key, values.get(key).toString().getBytes());
		}
		return retVal;
	}

	/** Return code when operation succeeded */
	public static final int SUCCESS = 0;

	/** Return code when operation did not succeed */
	public static final int ERROR = -1;

	public void printScenario() throws DBException {
		System.out.println("DataClass "+ dataClass.getName());
		System.out.println("Payload Class Type "+ payloadClassType);
		System.out.println("Batch Mode "+ batchMode);
		System.out.println("Batch Size "+ batchSize);
		
	}
}
