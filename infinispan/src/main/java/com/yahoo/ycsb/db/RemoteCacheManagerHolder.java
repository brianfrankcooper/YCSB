package com.yahoo.ycsb.db;

import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;

public class RemoteCacheManagerHolder {
	
	private static RemoteCacheManager cacheManager = null;
	
	private RemoteCacheManagerHolder() {}
	
	public static RemoteCacheManager getInstance(Properties props){
		if(cacheManager == null){
			synchronized (RemoteCacheManager.class) {
				cacheManager = new RemoteCacheManager(props);
			}
		}
		return cacheManager;
	}
}
