package com.yahoo.ycsb.db;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.sun.xml.internal.fastinfoset.sax.Properties;

public class GemFireServer {

	public static void main(String[] args) throws Exception{
		
		Cache cache = new CacheFactory()
        .set("name", "server")
        .set("cache-xml-file", "C://tools//eclipse361/git//YCSB//gemfire//src//main//conf//cache.xml")
        .create();

		// Get the exampleRegion
		Region<String, byte[]> exampleRegion = cache.getRegion("usertable");	  
		while (true)
		{
			Thread.sleep(10000);
		}
	}

}
