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
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static com.yahoo.ycsb.db.elasticsearch5.Elasticsearch5.KEY;
import static com.yahoo.ycsb.db.elasticsearch5.Elasticsearch5.parseIntegerProperty;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Elasticsearch REST client for YCSB framework.
 */
public class ElasticsearchRestClient extends DB {

  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9200";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private RestClient restClient;
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

    final String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");

    final List<HttpHost> esHttpHosts = new ArrayList<>(nodeList.length);
    for (String h : nodeList) {
      String[] nodes = h.split(":");
      esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "http"));
    }

    restClient = RestClient.builder(esHttpHosts.toArray(new HttpHost[esHttpHosts.size()])).build();

    final Response existsResponse = performRequest(restClient, "HEAD", "/" + indexKey);
    final boolean exists = existsResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK;

    if (exists && newIndex) {
      final Response deleteResponse = performRequest(restClient, "DELETE", "/" + indexKey);
      final int statusCode = deleteResponse.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        throw new DBException("delete [" + indexKey + "] failed with status [" + statusCode + "]");
      }
    }

    if (!exists || newIndex) {
      try (XContentBuilder builder = jsonBuilder()) {
        builder.startObject();
        builder.startObject("settings");
        builder.field("index.number_of_shards", numberOfShards);
        builder.field("index.number_of_replicas", numberOfReplicas);
        builder.endObject();
        builder.endObject();
        final Map<String, String> params = emptyMap();
        final StringEntity entity = new StringEntity(builder.string());
        final Response createResponse = performRequest(restClient, "PUT", "/" + indexKey, params, entity);
        final int statusCode = createResponse.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
          throw new DBException("create [" + indexKey + "] failed with status [" + statusCode + "]");
        }
      } catch (final IOException e) {
        throw new DBException(e);
      }
    }

    final Map<String, String> params = Collections.singletonMap("wait_for_status", "green");
    final Response healthResponse = performRequest(restClient, "GET", "/_cluster/health/" + indexKey, params);
    final int healthStatusCode = healthResponse.getStatusLine().getStatusCode();
    if (healthStatusCode != HttpStatus.SC_OK) {
      throw new DBException("cluster health [" + indexKey + "] failed with status [" + healthStatusCode + "]");
    }
  }

  private static Response performRequest(
          final RestClient restClient,
          final String method,
          final String endpoint) throws DBException {
    final Map<String, String> params = emptyMap();
    return performRequest(restClient, method, endpoint, params);
  }

  private static Response performRequest(
          final RestClient restClient,
          final String method,
          final String endpoint,
          final Map<String, String> params) throws DBException {
    return performRequest(restClient, method, endpoint, params, null);
  }

  private static final Header[] EMPTY_HEADERS = new Header[0];

  private static Response performRequest(
          final RestClient restClient,
          final String method,
          final String endpoint,
          final Map<String, String> params,
          final HttpEntity entity) throws DBException {
    try {
      final Header[] headers;
      if (entity != null) {
        headers = new Header[]{new BasicHeader("content-type", ContentType.APPLICATION_JSON.toString())};
      } else {
        headers = EMPTY_HEADERS;
      }
      return restClient.performRequest(
              method,
              endpoint,
              params,
              entity,
              headers);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (restClient != null) {
      try {
        restClient.close();
        restClient = null;
      } catch (final IOException e) {
        throw new DBException(e);
      }
    }
  }

  private volatile boolean isRefreshNeeded = false;
  
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final Map<String, String> data = StringByteIterator.getStringMap(values);
      data.put(KEY, key);

      final Response response = restClient.performRequest(
          "POST",
          "/" + indexKey + "/" + table + "/",
          Collections.<String, String>emptyMap(),
          new NStringEntity(new ObjectMapper().writeValueAsString(data), ContentType.APPLICATION_JSON));

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
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
      final Response searchResponse = search(table, key);
      final int statusCode = searchResponse.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
        return Status.NOT_FOUND;
      } else if (statusCode != HttpStatus.SC_OK) {
        return Status.ERROR;
      }

      final Map<String, Object> map = map(searchResponse);
      @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>)map.get("hits");
      final int total = (int)hits.get("total");
      if (total == 0) {
        return Status.NOT_FOUND;
      }
      @SuppressWarnings("unchecked") final Map<String, Object> hit =
              (Map<String, Object>)((List<Object>)hits.get("hits")).get(0);
      final Response deleteResponse =
              restClient.performRequest("DELETE", "/" + indexKey + "/" + table + "/" + hit.get("_id"));
      if (deleteResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
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
  public Status read(
          final String table,
          final String key,
          final Set<String> fields,
          final Map<String, ByteIterator> result) {
    try {
      final Response searchResponse = search(table, key);
      final int statusCode = searchResponse.getStatusLine().getStatusCode();
      if (statusCode == 404) {
        return Status.NOT_FOUND;
      } else if (statusCode != HttpStatus.SC_OK) {
        return Status.ERROR;
      }

      final Map<String, Object> map = map(searchResponse);
      @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>)map.get("hits");
      final int total = (int)hits.get("total");
      if (total == 0) {
        return Status.NOT_FOUND;
      }
      @SuppressWarnings("unchecked") final Map<String, Object> hit =
              (Map<String, Object>)((List<Object>)hits.get("hits")).get(0);
      @SuppressWarnings("unchecked") final Map<String, Object> source = (Map<String, Object>)hit.get("_source");
      if (fields != null) {
        for (final String field : fields) {
          result.put(field, new StringByteIterator((String) source.get(field)));
        }
      } else {
        for (final Map.Entry<String, Object> e : source.entrySet()) {
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
      final Response searchResponse = search(table, key);
      final int statusCode = searchResponse.getStatusLine().getStatusCode();
      if (statusCode == 404) {
        return Status.NOT_FOUND;
      } else if (statusCode != HttpStatus.SC_OK) {
        return Status.ERROR;
      }

      final Map<String, Object> map = map(searchResponse);
      @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
      final int total = (int) hits.get("total");
      if (total == 0) {
        return Status.NOT_FOUND;
      }
      @SuppressWarnings("unchecked") final Map<String, Object> hit =
              (Map<String, Object>) ((List<Object>) hits.get("hits")).get(0);
      @SuppressWarnings("unchecked") final Map<String, Object> source = (Map<String, Object>) hit.get("_source");
      for (final Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        source.put(entry.getKey(), entry.getValue());
      }
      final Map<String, String> params = emptyMap();
      final Response response = restClient.performRequest(
              "PUT",
              "/" + indexKey + "/" + table + "/" + hit.get("_id"),
              params,
              new NStringEntity(new ObjectMapper().writeValueAsString(source), ContentType.APPLICATION_JSON));
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
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
      final Response response;
      try (XContentBuilder builder = jsonBuilder()) {
        builder.startObject();
        builder.startObject("query");
        builder.startObject("range");
        builder.startObject(KEY);
        builder.field("gte", startkey);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.field("size", recordcount);
        builder.endObject();
        response = search(table, builder);
        @SuppressWarnings("unchecked") final Map<String, Object> map = map(response);
        @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>)map.get("hits");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> list =
                (List<Map<String, Object>>) hits.get("hits");

        for (final Map<String, Object> hit : list) {
          @SuppressWarnings("unchecked") final Map<String, Object> source = (Map<String, Object>)hit.get("_source");
          final HashMap<String, ByteIterator> entry;
          if (fields != null) {
            entry = new HashMap<>(fields.size());
            for (final String field : fields) {
              entry.put(field, new StringByteIterator((String) source.get(field)));
            }
          } else {
            entry = new HashMap<>(hit.size());
            for (final Map.Entry<String, Object> field : source.entrySet()) {
              if (KEY.equals(field.getKey())) {
                continue;
              }
              entry.put(field.getKey(), new StringByteIterator((String) field.getValue()));
            }
          }
          result.add(entry);
        }
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void refreshIfNeeded() throws IOException {
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
        restClient.performRequest("POST", "/" + indexKey + "/_refresh");
      }
    }
  }

  private Response search(final String table, final String key) throws IOException {
    try (XContentBuilder builder = jsonBuilder()) {
      builder.startObject();
      builder.startObject("query");
      builder.startObject("term");
      builder.field(KEY, key);
      builder.endObject();
      builder.endObject();
      builder.endObject();
      return search(table, builder);
    }
  }

  private Response search(final String table, final XContentBuilder builder) throws IOException {
    refreshIfNeeded();
    final Map<String, String> params = emptyMap();
    final StringEntity entity = new StringEntity(builder.string());
    final Header header = new BasicHeader("content-type", ContentType.APPLICATION_JSON.toString());
    return restClient.performRequest("GET", "/" + indexKey + "/" + table + "/_search", params, entity, header);
  }

  private Map<String, Object> map(final Response response) throws IOException {
    try (InputStream is = response.getEntity().getContent()) {
      final ObjectMapper mapper = new ObjectMapper();
      @SuppressWarnings("unchecked") final Map<String, Object> map = mapper.readValue(is, Map.class);
      return map;
    }
  }

}
