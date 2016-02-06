package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;

/**
 * Typical RESTFul services benchmarking scenario. Represents a set of client
 * calling REST operations like HTTP DELETE, GET, POST, PUT on a web service.
 * This scenario is completely different from CoreWorkload which is mainly
 * designed for databases benchmarking.
 * 
 * @author shivam.maharshi
 */
public class RestWorkload extends Workload {

	@Override
	public void init(Properties p) throws WorkloadException {
		
	};

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		return false;
	}

}
