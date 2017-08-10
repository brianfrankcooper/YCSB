/*
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.elasticsearch5;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static com.yahoo.ycsb.db.elasticsearch5.Elasticsearch5.KEY;
import static com.yahoo.ycsb.db.elasticsearch5.Elasticsearch5.parseIntegerProperty;
import static org.elasticsearch.common.settings.Settings.Builder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Elasticsearch client for YCSB framework.
 */
public class ElasticsearchClient extends DB {

  private static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9300";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private TransportClient client;
  private String indexKey;

  /**
   *
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);

    final int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    final int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);

    final Boolean newIndex = Boolean.parseBoolean(props.getProperty("es.new_index", "false"));
    final Builder settings = Settings.builder().put("cluster.name", DEFAULT_CLUSTER_NAME);

    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    for (final Entry<Object, Object> e : props.entrySet()) {
      if (e.getKey() instanceof String) {
        final String key = (String) e.getKey();
        if (key.startsWith("es.setting.")) {
          settings.put(key.substring("es.setting.".length()), e.getValue());
        }
      }
    }

    settings.put("client.transport.sniff", true)
            .put("client.transport.ignore_cluster_name", false)
            .put("client.transport.ping_timeout", "30s")
            .put("client.transport.nodes_sampler_interval", "30s");
    // Default it to localhost:9300
    final String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
    client = new PreBuiltTransportClient(settings.build());
    for (String h : nodeList) {
      String[] nodes = h.split(":");

      final InetAddress address;
      try {
        address = InetAddress.getByName(nodes[0]);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("unable to identity host [" + nodes[0]+ "]", e);
      }
      final int port;
      try {
        port = Integer.parseInt(nodes[1]);
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("unable to parse port [" + nodes[1] + "]", e);
      }
      client.addTransportAddress(new InetSocketTransportAddress(address, port));
    }

    final boolean exists =
        client.admin().indices()
            .exists(Requests.indicesExistsRequest(indexKey)).actionGet()
            .isExists();
    if (exists && newIndex) {
      client.admin().indices().prepareDelete(indexKey).get();
    }
    if (!exists || newIndex) {
      client.admin().indices().create(
          new CreateIndexRequest(indexKey)
              .settings(
                  Settings.builder()
                      .put("index.number_of_shards", numberOfShards)
                      .put("index.number_of_replicas", numberOfReplicas)
              )).actionGet();
    }
    client.admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet();
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  private volatile boolean isRefreshNeeded = false;

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try (XContentBuilder doc = jsonBuilder()) {

      doc.startObject();
      for (final Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }
      doc.field(KEY, key);
      doc.endObject();

      final IndexResponse indexResponse = client.prepareIndex(indexKey, table).setSource(doc).get();
      if (indexResponse.getResult() != DocWriteResponse.Result.CREATED) {
        return Status.ERROR;
      }

      if (!isRefreshNeeded) {
        synchronized (this) {
          isRefreshNeeded = true;
        }
      }

      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      final SearchResponse searchResponse = search(table, key);
      if (searchResponse.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final String id = searchResponse.getHits().getAt(0).getId();

      final DeleteResponse deleteResponse = client.prepareDelete(indexKey, table, id).get();
      if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
        return Status.NOT_FOUND;
      }

      if (!isRefreshNeeded) {
        synchronized (this) {
          isRefreshNeeded = true;
        }
      }

      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status read(
          final String table,
          final String key,
          final Set<String> fields,
          final Map<String, ByteIterator> result) {
    try {
      final SearchResponse searchResponse = search(table, key);
      if (searchResponse.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final SearchHit hit = searchResponse.getHits().getAt(0);
      if (fields != null) {
        for (final String field : fields) {
          result.put(field, new StringByteIterator((String) hit.getSource().get(field)));
        }
      } else {
        for (final Map.Entry<String, Object> e : hit.getSource().entrySet()) {
          if (KEY.equals(e.getKey())) {
            continue;
          }
          result.put(e.getKey(), new StringByteIterator((String) e.getValue()));
        }
      }

      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final SearchResponse response = search(table, key);
      if (response.getHits().totalHits == 0) {
        return Status.NOT_FOUND;
      }

      final SearchHit hit = response.getHits().getAt(0);
      for (final Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        hit.getSource().put(entry.getKey(), entry.getValue());
      }

      final IndexResponse indexResponse =
              client.prepareIndex(indexKey, table, hit.getId()).setSource(hit.getSource()).get();

      if (indexResponse.getResult() != DocWriteResponse.Result.UPDATED) {
        return Status.ERROR;
      }

      if (!isRefreshNeeded) {
        synchronized (this) {
          isRefreshNeeded = true;
        }
      }

      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      final String table,
      final String startkey,
      final int recordcount,
      final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    try {
      refreshIfNeeded();
      final RangeQueryBuilder query = new RangeQueryBuilder(KEY).gte(startkey);
      final SearchResponse response = client.prepareSearch(indexKey).setQuery(query).setSize(recordcount).get();

      for (final SearchHit hit : response.getHits()) {
        final HashMap<String, ByteIterator> entry;
        if (fields != null) {
          entry = new HashMap<>(fields.size());
          for (final String field : fields) {
            entry.put(field, new StringByteIterator((String) hit.getSource().get(field)));
          }
        } else {
          entry = new HashMap<>(hit.getSource().size());
          for (final Map.Entry<String, Object> field : hit.getSource().entrySet()) {
            if (KEY.equals(field.getKey())) {
              continue;
            }
            entry.put(field.getKey(), new StringByteIterator((String) field.getValue()));
          }
        }
        result.add(entry);
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void refreshIfNeeded() {
    if (isRefreshNeeded) {
      final boolean refresh;
      synchronized (this) {
        if (isRefreshNeeded) {
          refresh = true;
          isRefreshNeeded = false;
        } else {
          refresh = false;
        }
      }
      if (refresh) {
        client.admin().indices().refresh(new RefreshRequest()).actionGet();
      }
    }
  }

  private SearchResponse search(final String table, final String key) {
    refreshIfNeeded();
    return client.prepareSearch(indexKey).setTypes(table).setQuery(new TermQueryBuilder(KEY, key)).get();
  }

}
