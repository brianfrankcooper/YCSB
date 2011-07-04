package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/**
 * This is a client implementation for Infinispan 5.x.
 *
 * @author Manik Surtani (manik AT jboss DOT org)
 */
public class InfinispanClient extends DB {

   private static final int OK = 0;
   private static final int ERROR = -1;
   private static final int NOT_FOUND = -2;

   private EmbeddedCacheManager infinispanManager;

   private static final Log logger = LogFactory.getLog(InfinispanClient.class);

   @Override
   public void init() throws DBException {
      try {
         infinispanManager = new DefaultCacheManager("infinispan-config.xml");
      } catch (IOException e) {
         throw new DBException(e);
      }
   }

   @Override
   public void cleanup() {
      infinispanManager.stop();
      infinispanManager = null;
   }

   @Override
   public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
      try {
         AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key, false);
         if (row != null) {
            result.clear();
            if (fields == null || fields.isEmpty()) {
               result.putAll(row);
            } else {
               for (String field : fields) result.put(field, row.get(field));
            }
         }
         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   @Override
   public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, String>> result) {
      logger.warn("Infinispan does not support scan semantics");
      return OK;
   }

   @Override
   public int update(String table, String key, HashMap<String, String> values) {
      try {
         AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
         row.putAll(values);

         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   @Override
   public int insert(String table, String key, HashMap<String, String> values) {
      try {
         AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
         row.clear();
         row.putAll(values);

         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }

   @Override
   public int delete(String table, String key) {
      try {
         AtomicMapLookup.removeAtomicMap(infinispanManager.getCache(table), key);
         return OK;
      } catch (Exception e) {
         return ERROR;
      }
   }
}
