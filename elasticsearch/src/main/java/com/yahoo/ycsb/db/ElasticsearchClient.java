/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.db;

import static org.elasticsearch.common.settings.Settings.Builder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Elasticsearch client for YCSB framework.
 *
 * <p>
 * Default properties to set:
 * </p>
 * <ul>
 * <li>es.cluster.name = es.ycsb.cluster
 * <li>es.client = true
 * <li>es.index.key = es.ycsb
 * </ul>
 *
 * @author Sharmarke Aden
 *
 */
public class ElasticsearchClient extends DB {

  public static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
  public static final String DEFAULT_INDEX_KEY = "es.ycsb";
  public static final String DEFAULT_REMOTE_HOST = "localhost:9300";
  private Node node;
  private Client client;
  private String indexKey;
  private Boolean remoteMode;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);
    String clusterName =
        props.getProperty("cluster.name", DEFAULT_CLUSTER_NAME);
    // Check if transport client needs to be used (To connect to multiple
    // elasticsearch nodes)
    remoteMode = Boolean
        .parseBoolean(props.getProperty("elasticsearch.remote", "false"));
    Boolean newdb =
        Boolean.parseBoolean(props.getProperty("elasticsearch.newdb", "false"));
    Builder settings = Settings.settingsBuilder()
        .put("node.local", "true")
        .put("path.data", System.getProperty("java.io.tmpdir") + "/esdata")
        .put("discovery.zen.ping.multicast.enabled", "false")
        .put("index.mapping._id.indexed", "true")
        .put("index.gateway.type", "none")
        .put("index.number_of_shards", "1")
        .put("index.number_of_replicas", "0")
        .put("path.home", System.getProperty("java.io.tmpdir"));

    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    settings.put(props);
    System.out.println(
        "Elasticsearch starting node = " + settings.get("cluster.name"));
    System.out
        .println("Elasticsearch node data path = " + settings.get("path.data"));
    System.out.println("Elasticsearch Remote Mode = " + remoteMode);
    // Remote mode support for connecting to remote elasticsearch cluster
    if (remoteMode) {
      settings.put("client.transport.sniff", true)
          .put("client.transport.ignore_cluster_name", false)
          .put("client.transport.ping_timeout", "30s")
          .put("client.transport.nodes_sampler_interval", "30s");
      // Default it to localhost:9300
      String[] nodeList =
          props.getProperty("elasticsearch.hosts.list", DEFAULT_REMOTE_HOST)
              .split(",");
      System.out.println("Elasticsearch Remote Hosts = "
          + props.getProperty("elasticsearch.hosts.list", DEFAULT_REMOTE_HOST));
      TransportClient tClient = TransportClient.builder()
                                  .settings(settings).build();
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        try {
          tClient.addTransportAddress(new InetSocketTransportAddress(
              InetAddress.getByName(nodes[0]),
              Integer.parseInt(nodes[1])
              ));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Unable to parse port number.", e);
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException("Unable to Identify host.", e);
        }
      }
      client = tClient;
    } else { // Start node only if transport client mode is disabled
      node = nodeBuilder().clusterName(clusterName).settings(settings).node();
      node.start();
      client = node.client();
    }

    //wait for shards to be ready
    client.admin().cluster()
      .health(new ClusterHealthRequest("lists").waitForActiveShards(1))
      .actionGet();
    if (newdb) {
      client.admin().indices().prepareDelete(indexKey).execute().actionGet();
      client.admin().indices().prepareCreate(indexKey).execute().actionGet();
    } else {
      boolean exists = client.admin().indices()
          .exists(Requests.indicesExistsRequest(indexKey)).actionGet()
          .isExists();
      if (!exists) {
        client.admin().indices().prepareCreate(indexKey).execute().actionGet();
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (!remoteMode) {
      if (!node.isClosed()) {
        client.close();
        node.close();
      }
    } else {
      client.close();
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      final XContentBuilder doc = jsonBuilder().startObject();

      for (Entry<String, String> entry : StringByteIterator.getStringMap(values)
          .entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }

      doc.endObject();

      client.prepareIndex(indexKey, table, key).setSource(doc).execute()
          .actionGet();

      return Status.OK;
    } catch (Exception e) {
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
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      client.prepareDelete(indexKey, table, key).execute().actionGet();
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
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
      final GetResponse response =
          client.prepareGet(indexKey, table, key).execute().actionGet();

      if (response.isExists()) {
        if (fields != null) {
          for (String field : fields) {
            result.put(field, new StringByteIterator(
                (String) response.getSource().get(field)));
          }
        } else {
          for (String field : response.getSource().keySet()) {
            result.put(field, new StringByteIterator(
                (String) response.getSource().get(field)));
          }
        }
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      final GetResponse response =
          client.prepareGet(indexKey, table, key).execute().actionGet();

      if (response.isExists()) {
        for (Entry<String, String> entry : StringByteIterator
            .getStringMap(values).entrySet()) {
          response.getSource().put(entry.getKey(), entry.getValue());
        }

        client.prepareIndex(indexKey, table, key)
            .setSource(response.getSource()).execute().actionGet();

        return Status.OK;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
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
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      final RangeQueryBuilder rangeQuery = rangeQuery("_id").gte(startkey);
      final SearchResponse response = client.prepareSearch(indexKey)
          .setTypes(table)
          .setQuery(rangeQuery)
          .setSize(recordcount)
          .execute()
          .actionGet();

      HashMap<String, ByteIterator> entry;

      for (SearchHit hit : response.getHits()) {
        entry = new HashMap<String, ByteIterator>(fields.size());

        for (String field : fields) {
          entry.put(field,
              new StringByteIterator((String) hit.getSource().get(field)));
        }

        result.add(entry);
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
