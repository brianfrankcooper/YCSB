package com.yahoo.ycsb.db;

import java.util.*;

import net.citrusleaf.CitrusleafClient;
import net.citrusleaf.CitrusleafClient.ClResult;
import net.citrusleaf.CitrusleafClient.ClResultCode;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public class AerospikeClient extends com.yahoo.ycsb.DB{

    public static final int OK = 0;
    public static final int NULL_RESULT = -20;
    /**
     *  Mapping of ClResultCodes to documented Client Return Error codes
     *  @see http://www.aerospike.com/quick-install/documentation/troubleshooting-guide/#clientcodes
     */
	private static final Map<ClResultCode, Integer> RESULT_CODE_MAPPER;
    static {
        RESULT_CODE_MAPPER = new EnumMap<ClResultCode, Integer>(ClResultCode.class);

        RESULT_CODE_MAPPER.put(ClResultCode.SERVER_ERROR, 1);
        RESULT_CODE_MAPPER.put(ClResultCode.KEY_NOT_FOUND_ERROR, 2);
        RESULT_CODE_MAPPER.put(ClResultCode.GENERATION_ERROR, 3);
        RESULT_CODE_MAPPER.put(ClResultCode.PARAMETER_ERROR, 4);
        RESULT_CODE_MAPPER.put(ClResultCode.KEY_EXISTS_ERROR, 5);
        RESULT_CODE_MAPPER.put(ClResultCode.BIN_EXISTS_ERROR, 6);
        RESULT_CODE_MAPPER.put(ClResultCode.CLUSTER_KEY_MISMATCH, 7);
        RESULT_CODE_MAPPER.put(ClResultCode.SERVER_MEM_ERROR, 8);
        RESULT_CODE_MAPPER.put(ClResultCode.TIMEOUT, 9);
        RESULT_CODE_MAPPER.put(ClResultCode.NO_XDS, 10);
        RESULT_CODE_MAPPER.put(ClResultCode.SERVER_NOT_AVAILABLE, 11);
        RESULT_CODE_MAPPER.put(ClResultCode.BIN_TYPE_ERROR, 12);
        RESULT_CODE_MAPPER.put(ClResultCode.RECORD_TOO_BIG, 13);
        RESULT_CODE_MAPPER.put(ClResultCode.KEY_BUSY, 14);

        RESULT_CODE_MAPPER.put(ClResultCode.OK, OK);
        RESULT_CODE_MAPPER.put(ClResultCode.CLIENT_ERROR, -4);
        RESULT_CODE_MAPPER.put(ClResultCode.SERIALIZE_ERROR, -10);

        RESULT_CODE_MAPPER.put(ClResultCode.NOT_SET, -1);
    }

    private CitrusleafClient cl;

	public static  String NAMESPACE = "test";

	public static  String SET = "YCSB";

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

		cl = new CitrusleafClient(host, port);
		try {
			//Sleep so that the partition hashmap is created by the client
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		
		if (!cl.isConnected()) {
			throw new DBException(String.format("Failed to add %s:%d to cluster.", 
				host, port));
		}

	}

		public void cleanup() throws DBException {
		cl.close();
	}

    @Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		if(fields != null) {
			for (String bin : fields) {
				ClResult res = cl.get(NAMESPACE, SET, key, bin, null);
				if (res.result != null){
					result.put(bin, new ByteArrayByteIterator(res.result.toString().getBytes()));
				} else {
					return NULL_RESULT;
                }
                if (res.resultCode != ClResultCode.OK) {
                    return RESULT_CODE_MAPPER.get(res.resultCode);
                }
			}
            return OK;
		}
		else {
			ClResult res = cl.getAll(NAMESPACE, SET, key, null);
			if (res.resultCode == ClResultCode.OK) {
				for(Map.Entry<String, Object> entry : res.results.entrySet()) {
					result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().toString().getBytes()));
				}
			}
			return RESULT_CODE_MAPPER.get(res.resultCode);
		}
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		Map<String, Object> v = new HashMap<String, Object>();

		for (Map.Entry<String, ByteIterator> entry : values.entrySet()){
			v.put(entry.getKey(), entry.getValue().toString());
		}

		ClResultCode rc = cl.set(NAMESPACE, SET, key, v, null, null);

		return RESULT_CODE_MAPPER.get(rc);
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		Map<String, Object> v = new HashMap<String, Object>();

        for (Map.Entry<String, ByteIterator> entry : values.entrySet()){
            v.put(entry.getKey(), entry.getValue().toString());
        }

        ClResultCode rc = cl.set(NAMESPACE, SET, key, v, null, null);

        return RESULT_CODE_MAPPER.get(rc);
	}

	@Override
	public int delete(String table, String key) {
        ClResultCode rc = cl.delete(NAMESPACE, SET, key, null, null);
        return RESULT_CODE_MAPPER.get(rc);
	}

}


