package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * This is a client implementation for Infinispan 5.x in client-server mode.
 * 
 * @author mylesjao
 *
 */
public class InfinispanRemoteClient extends DB {

   private RemoteCacheManager remoteIspnManager;
   
   private String cacheName = null;

   private static final Log logger = LogFactory.getLog(InfinispanRemoteClient.class);

   public InfinispanRemoteClient() {
      
   }
   
   @Override
   public void init() throws DBException {
	  remoteIspnManager = RemoteCacheManagerHolder.getInstance(getProperties());
	  cacheName = getProperties().getProperty("cache");
   }
   
   @Override
   public void cleanup() {
      remoteIspnManager.stop();
      remoteIspnManager = null;
   }
   
   @Override
   public int insert(String table, String recordKey, HashMap<String, ByteIterator> values) {
	   String compositKey = createKey(table, recordKey);
	   Map<String, String> stringValues = new HashMap<String,String>();
	   StringByteIterator.putAllAsStrings(stringValues, values);
	   try {
    	  cache().put(compositKey, stringValues);
         return Status.OK;
      } catch (Exception e) {
         return Status.ERROR;
      }
   }
   
   @Override
   public int read(String table, String recordKey, Set<String> fields, HashMap<String, ByteIterator> result) {
	   String compositKey = createKey(table, recordKey);
	   try {	  
    	  Map<String, String> values = cache().get(compositKey);
    	  
    	  if(values == null || values.isEmpty()){
    		  return Status.NOT_FOUND;
    	  }
    	  
    	  if(fields == null){ //get all field/value pairs
    		  StringByteIterator.putAllAsByteIterators(result, values);
    	  }else{
    		  for(String field: fields){
    			  String value = values.get(field);
    			  if(value != null){
    				  result.put(field, new StringByteIterator(value) );
    			  }
    		  }
    	  }
    	  
    	  return Status.OK;
      } catch (Exception e) {
         return Status.ERROR;
      }
   }
   
   @Override
   public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      logger.warn("Infinispan does not support scan semantics");
      return Status.NOT_SUPPORT;
   }
   
   @Override
   public int update(String table, String recordKey, HashMap<String, ByteIterator> values) {
	   String compositKey = createKey(table, recordKey);
      try {
    	  Map<String, String> stringValues = new HashMap<String, String>();
    	  StringByteIterator.putAllAsStrings(stringValues, values);
    	  cache().put(compositKey, stringValues);
         return Status.OK;
      } catch (Exception e) {
         return Status.ERROR;
      }
   }
   @Override
   public int delete(String table, String recordKey) {
	   String compositKey = createKey(table, recordKey);
      try {
    	  cache().remove(compositKey);
    	  return Status.OK;
      } catch (Exception e) {
         return Status.ERROR;
      }
   }
   
   private RemoteCache<String, Map<String,String>> cache(){
	   if(this.cacheName != null){
		   return remoteIspnManager.getCache(cacheName);
	   }else{
		   return remoteIspnManager.getCache();
	   }
   }
   
   private String createKey(String table, String recordKey){
	   return table + "-" + recordKey;
   }
}
