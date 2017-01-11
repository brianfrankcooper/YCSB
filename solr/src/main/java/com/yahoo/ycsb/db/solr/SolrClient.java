/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
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

package com.yahoo.ycsb.db.solr;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Solr client for YCSB framework.
 *
 * <p>
 * Default properties to set:
 * </p>
 * <ul>
 * See README.md
 * </ul>
 *
 */
public class SolrClient extends DB {

  public static final String DEFAULT_CLOUD_MODE = "false";
  public static final String DEFAULT_BATCH_MODE = "false";
  public static final String DEFAULT_ZOOKEEPER_HOSTS = "localhost:2181";
  public static final String DEFAULT_SOLR_BASE_URL = "http://localhost:8983/solr";
  public static final String DEFAULT_COMMIT_WITHIN_TIME = "1000";

  private org.apache.solr.client.solrj.SolrClient client;
  private Integer commitTime;
  private Boolean batchMode;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    commitTime = Integer
        .parseInt(props.getProperty("solr.commit.within.time", DEFAULT_COMMIT_WITHIN_TIME));
    batchMode = Boolean.parseBoolean(props.getProperty("solr.batch.mode", DEFAULT_BATCH_MODE));


    String jaasConfPath = props.getProperty("solr.jaas.conf.path");
    if(jaasConfPath != null) {
      System.setProperty("java.security.auth.login.config", jaasConfPath);
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
    }

    // Check if Solr cluster is running in SolrCloud or Stand-alone mode
    Boolean cloudMode = Boolean.parseBoolean(props.getProperty("solr.cloud", DEFAULT_CLOUD_MODE));
    System.err.println("Solr Cloud Mode = " + cloudMode);
    if (cloudMode) {
      System.err.println("Solr Zookeeper Remote Hosts = "
          + props.getProperty("solr.zookeeper.hosts", DEFAULT_ZOOKEEPER_HOSTS));
      client = new CloudSolrClient(
          props.getProperty("solr.zookeeper.hosts", DEFAULT_ZOOKEEPER_HOSTS));
    } else {
      client = new HttpSolrClient(props.getProperty("solr.base.url", DEFAULT_SOLR_BASE_URL));
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      client.close();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   *         discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      SolrInputDocument doc = new SolrInputDocument();

      doc.addField("id", key);
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.addField(entry.getKey(), entry.getValue());
      }
      UpdateResponse response;
      if (batchMode) {
        response = client.add(table, doc, commitTime);
      } else {
        response = client.add(table, doc);
        client.commit(table);
      }
      return checkStatus(response.getStatus());

    } catch (IOException | SolrServerException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   *         discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      UpdateResponse response;
      if (batchMode) {
        response = client.deleteById(table, key, commitTime);
      } else {
        response = client.deleteById(table, key);
        client.commit(table);
      }
      return checkStatus(response.getStatus());
    } catch (IOException | SolrServerException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a
   * HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      Boolean returnFields = false;
      String[] fieldList = null;
      if (fields != null) {
        returnFields = true;
        fieldList = fields.toArray(new String[fields.size()]);
      }
      SolrQuery query = new SolrQuery();
      query.setQuery("id:" + key);
      if (returnFields) {
        query.setFields(fieldList);
      }
      final QueryResponse response = client.query(table, query);
      SolrDocumentList results = response.getResults();
      if ((results != null) && (results.getNumFound() > 0)) {
        for (String field : results.get(0).getFieldNames()) {
          result.put(field,
              new StringByteIterator(String.valueOf(results.get(0).getFirstValue(field))));
        }
      }
      return checkStatus(response.getStatus());
    } catch (IOException | SolrServerException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   *         discussion of error codes.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      SolrInputDocument updatedDoc = new SolrInputDocument();
      updatedDoc.addField("id", key);

      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        updatedDoc.addField(entry.getKey(), Collections.singletonMap("set", entry.getValue()));
      }

      UpdateResponse writeResponse;
      if (batchMode) {
        writeResponse = client.add(table, updatedDoc, commitTime);
      } else {
        writeResponse = client.add(table, updatedDoc);
        client.commit(table);
      }
      return checkStatus(writeResponse.getStatus());
    } catch (IOException | SolrServerException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   *         discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      Boolean returnFields = false;
      String[] fieldList = null;
      if (fields != null) {
        returnFields = true;
        fieldList = fields.toArray(new String[fields.size()]);
      }
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");
      query.setParam("fq", "id:[ " + startkey + " TO * ]");
      if (returnFields) {
        query.setFields(fieldList);
      }
      query.setRows(recordcount);
      final QueryResponse response = client.query(table, query);
      SolrDocumentList results = response.getResults();

      HashMap<String, ByteIterator> entry;

      for (SolrDocument hit : results) {
        entry = new HashMap<String, ByteIterator>((int) results.getNumFound());
        for (String field : hit.getFieldNames()) {
          entry.put(field, new StringByteIterator(String.valueOf(hit.getFirstValue(field))));
        }
        result.add(entry);
      }
      return checkStatus(response.getStatus());
    } catch (IOException | SolrServerException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  private Status checkStatus(int status) {
    Status responseStatus;
    switch (status) {
    case 0:
      responseStatus = Status.OK;
      break;
    case 400:
      responseStatus = Status.BAD_REQUEST;
      break;
    case 403:
      responseStatus = Status.FORBIDDEN;
      break;
    case 404:
      responseStatus = Status.NOT_FOUND;
      break;
    case 500:
      responseStatus = Status.ERROR;
      break;
    case 503:
      responseStatus = Status.SERVICE_UNAVAILABLE;
      break;
    default:
      responseStatus = Status.UNEXPECTED_STATE;
      break;
    }
    return responseStatus;
  }

}
