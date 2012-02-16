package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * This is a client implementation for Infinispan 5.x.
 *
 * Some settings:
 *
 * @author Manik Surtani (manik AT jboss DOT org)
 */
public class InfinispanClient extends DB {

   private static final int OK = 0;
   private static final int ERROR = -1;
   private static final int NOT_FOUND = -2;

   // An optimisation for clustered mode
   private final boolean clustered;

   private EmbeddedCacheManager infinispanManager;

   private static final Log logger = LogFactory.getLog(InfinispanClient.class);

   public InfinispanClient() {
      clustered = Boolean.getBoolean("infinispan.clustered");
   }

   public void init() throws DBException {
      try {
         infinispanManager = new DefaultCacheManager("infinispan-config.xml");
      } catch (IOException e) {
         throw new DBException(e);
      }
   }

   public void cleanup() {
      infinispanManager.stop();
      infinispanManager = null;
   }

   public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
      try {
         Map<String, String> row;
         if (clustered) {
            row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key, false);
         } else {
            Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
            row = cache.get(key);
         }
         if (row != null) {
            result.clear();
            if (fields == null || fields.isEmpty()) {
		StringByteIterator.putAllAsByteIterators(result, row);
            } else {
	       for (String field : fields) result.put(field, new StringByteIterator(row.get(field)));
            }
         }
         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      logger.warn("Infinispan does not support scan semantics");
      return OK;
   }

   public int update(String table, String key, HashMap<String, ByteIterator> values) {
      try {
         if (clustered) {
            AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
            StringByteIterator.putAllAsStrings(row, values);
         } else {
            Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
            Map<String, String> row = cache.get(key);
            if (row == null) {
               row = StringByteIterator.getStringMap(values);
               cache.put(key, row);
            } else {
               StringByteIterator.putAllAsStrings(row, values);
            }
         }

         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   public int insert(String table, String key, HashMap<String, ByteIterator> values) {
      try {
         if (clustered) {
            AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
            row.clear();
            StringByteIterator.putAllAsStrings(row, values);
         } else {
            infinispanManager.getCache(table).put(key, values);
         }

         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   public int delete(String table, String key) {
      try {
         if (clustered)
            AtomicMapLookup.removeAtomicMap(infinispanManager.getCache(table), key);
         else
            infinispanManager.getCache(table).remove(key);
         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }
}
