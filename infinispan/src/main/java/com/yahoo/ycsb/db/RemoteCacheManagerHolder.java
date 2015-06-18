package com.yahoo.ycsb.db;

import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;

public class RemoteCacheManagerHolder {
	
	private static volatile RemoteCacheManager cacheManager = null;
	
	private RemoteCacheManagerHolder() {}
	
	public static RemoteCacheManager getInstance(Properties props){
		RemoteCacheManager result = cacheManager;
		if(result == null){
			synchronized (RemoteCacheManagerHolder.class) {
				result = cacheManager;
				if (result == null) {
					cacheManager = result = new RemoteCacheManager(props);
				}
			}
		}
		return result;
	}
}
