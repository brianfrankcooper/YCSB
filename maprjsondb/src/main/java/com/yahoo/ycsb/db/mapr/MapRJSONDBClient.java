/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.mapr;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.ojai.Document;
import org.ojai.DocumentConstants;
import org.ojai.DocumentStream;
import org.ojai.Value;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Driver;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;

/**
 * MapR-DB(json) client for YCSB framework.
 *
 */
public class MapRJSONDBClient extends com.yahoo.ycsb.DB {

  private Connection connection = null;
  private DocumentStore documentStore = null;
  private Driver driver = null;

  @Override
  public void init() {
    connection = DriverManager.getConnection("ojai:mapr:");
    driver = connection.getDriver();
  }

  @Override
  public void cleanup() {
    documentStore.close();
    connection.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      DocumentStore docStore = getTable(table);
      Document doc = docStore.findById(key, getFieldPaths(fields));
      buildRowResult(doc, result);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      DocumentStore docStore = getTable(table);
      QueryCondition condition = driver.newCondition()
          .is(DocumentConstants.ID_FIELD, Op.GREATER_OR_EQUAL, startkey)
          .build();
      Query query = driver.newQuery()
          .select(getFieldPaths(fields))
          .where(condition)
          .build();
      
      try (DocumentStream stream =
          docStore.findQuery(query)) {
        int numResults = 0;
        for (Document record : stream) {
          result.add(buildRowResult(record));
          numResults++;
          if (numResults >= recordcount) {
            break;
          }
        }
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      DocumentStore docStore = getTable(table);
      docStore.update(key, newMutation(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      DocumentStore docStore = getTable(table);
      docStore.insertOrReplace(key, newDocument(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      DocumentStore docStore = getTable(table);
      docStore.delete(key);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * Get the OJAI DocumentStore instance for a given table.
   * 
   * @param tableName
   * @return
   */
  private DocumentStore getTable(String tableName) {
    if (documentStore == null) {
      documentStore = connection.getStore(tableName);
    }
    return documentStore;
  }

  /**
   * Construct a Document object from the map of OJAI values.  
   * 
   * @param values
   * @return
   */
  private Document newDocument(Map<String, ByteIterator> values) {
    Document document = driver.newDocument();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      document.set(entry.getKey(), entry.getValue().toArray());
    }
    return document;
  }

  /**
   * Build a DocumentMutation object for the values specified.
   * @param values
   * @return
   */
  private DocumentMutation newMutation(Map<String, ByteIterator> values) {
    DocumentMutation mutation = driver.newMutation();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      mutation.setOrReplace(entry.getKey(),
          ByteBuffer.wrap(entry.getValue().toArray()));
    }
    return mutation;
  }
  
  /**
   * Get field path array from the set.
   * 
   * @param fields
   * @return
   */
  private String[] getFieldPaths(Set<String> fields) {
    if (fields != null) {
      return fields.toArray(new String[fields.size()]);
    }
    return new String[0];
  }

  /**
   * Build result the map from the Document passed.
   *  
   * @param document
   * @return
   */
  private HashMap<String, ByteIterator> buildRowResult(Document document) {
    return buildRowResult(document, null);
  }
  
  /**
   * Build result the map from the Document passed.
   * 
   * @param document
   * @param result
   * @return
   */
  private  HashMap<String, ByteIterator> buildRowResult(Document document,
      Map<String, ByteIterator> result) {
    if (document != null) {
      if (result == null) {
        result = new HashMap<String, ByteIterator>();
      }
      for (Map.Entry<String, Value> kv : document) {
        result.put(kv.getKey(), new ValueByteIterator(kv.getValue()));
      }
    }
    return (HashMap<String, ByteIterator>)result;
  }
}
