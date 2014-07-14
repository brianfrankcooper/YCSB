package com.yahoo.ycsb.db;

import java.util.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public class AerospikeClient extends com.yahoo.ycsb.DB{

	public static final int OK = 0;
	public static final int NULL_RESULT = -20;
	private static final Map<Integer, Integer> RESULT_CODE_MAPPER;
	static {
		RESULT_CODE_MAPPER = new HashMap<Integer, Integer>();
		RESULT_CODE_MAPPER.put(ResultCode.SERVER_ERROR, 1);
		RESULT_CODE_MAPPER.put(ResultCode.KEY_NOT_FOUND_ERROR, 2);
		RESULT_CODE_MAPPER.put(ResultCode.GENERATION_ERROR, 3);
		RESULT_CODE_MAPPER.put(ResultCode.PARAMETER_ERROR, 4);
		RESULT_CODE_MAPPER.put(ResultCode.KEY_EXISTS_ERROR, 5);
		RESULT_CODE_MAPPER.put(ResultCode.BIN_EXISTS_ERROR, 6);
		RESULT_CODE_MAPPER.put(ResultCode.CLUSTER_KEY_MISMATCH, 7);
		RESULT_CODE_MAPPER.put(ResultCode.SERVER_MEM_ERROR, 8);
		RESULT_CODE_MAPPER.put(ResultCode.TIMEOUT, 9);
		RESULT_CODE_MAPPER.put(ResultCode.NO_XDS, 10);
		RESULT_CODE_MAPPER.put(ResultCode.SERVER_NOT_AVAILABLE, 11);
		RESULT_CODE_MAPPER.put(ResultCode.BIN_TYPE_ERROR, 12);
		RESULT_CODE_MAPPER.put(ResultCode.RECORD_TOO_BIG, 13);
		RESULT_CODE_MAPPER.put(ResultCode.KEY_BUSY, 14);
		RESULT_CODE_MAPPER.put(ResultCode.OK, OK);
		RESULT_CODE_MAPPER.put(ResultCode.SERIALIZE_ERROR, -10);
	}

	private com.aerospike.client.AerospikeClient as;
	public static  String NAMESPACE = "test";

	public static  String SET = "YCSB";

	private Policy policy = new Policy();
	private WritePolicy writePolicy = new WritePolicy();

	public void init() throws DBException {
		Properties props = getProperties();
		int port;

		//retrieve port
		String portString = props.getProperty("port");
		if (portString != null) {
			port = Integer.parseInt(portString);
		}
		else {
			port = 3000;
		}

		//retrieve host
		String host = props.getProperty("host");
		if(host == null) {
			host = "localhost";
		}
		
		//retrieve namespace
		String ns = props.getProperty("ns");
		if(ns !=  null ) {
			NAMESPACE = ns;
		}

		//retrieve set
		String st = props.getProperty("set");
		if(st != null) {
			SET = st;
		}

		try{
			as = new com.aerospike.client.AerospikeClient(host, port);
		} catch (AerospikeException e){
			throw new DBException(String.format("Failed to add %s:%d to cluster.", host, port));
		}
	}

	public void cleanup() throws DBException {
		as.close();
	}

	@Override
	public int readOne(String table, String key, String field, Map<String,ByteIterator> result) {
		try {
			as.get(policy, new Key(NAMESPACE, SET, key), field);
			return OK;
		} catch (AerospikeException e) {
			return RESULT_CODE_MAPPER.get(e.getResultCode());
		}
	}

	@Override
	public int readAll(String table, String key, Map<String,ByteIterator> result) {
		try {
			as.get(policy, new Key(NAMESPACE, SET, key));
			return OK;
		} catch (AerospikeException e) {
			return RESULT_CODE_MAPPER.get(e.getResultCode());
		}
	}

	@Override
	public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {

		System.out.println("Unsupported operation.");
		return -1;
	}

	@Override
	public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {

		System.out.println("Unsupported operation.");
		return -1;
	}

	@Override
	public int updateOne(String table, String key, String field, ByteIterator value) {

		return update(key, Collections.singletonMap(field, value));
	}

	@Override
	public int updateAll(String table, String key, Map<String,ByteIterator> values) {

		return update(key, values);
	}

	public int update(String key, Map<String, ByteIterator> values) {

		Bin[] bins = new Bin[values.size()];
		int index = 0;
		for (Map.Entry<String, ByteIterator> entry : values.entrySet() ) {
			bins[index] = new Bin(entry.getKey(), entry.getValue().toArray() );
			index++;
		}
		try {
			as.put(writePolicy, new Key(NAMESPACE, SET, key), bins);
			return OK;
		} catch (AerospikeException e){
			return e.getResultCode();
		}
	}

	@Override
	public int insert(String table, String key, Map<String, ByteIterator> values) {
		return updateAll(table, key, values);
	}

	@Override
	public int delete(String table, String key) {
		try {
			as.delete(writePolicy, new Key(NAMESPACE, SET, key));
			return OK;
		} catch (AerospikeException e){
			return RESULT_CODE_MAPPER.get(e.getResultCode());
		}
	}
}


