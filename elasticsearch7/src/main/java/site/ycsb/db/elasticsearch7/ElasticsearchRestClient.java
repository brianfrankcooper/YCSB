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

package site.ycsb.db.elasticsearch7;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.javatuples.Pair;
import site.ycsb.*;
import site.ycsb.workloads.CommerceWorkload;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static site.ycsb.db.elasticsearch7.Elasticsearch7.KEY;
import static site.ycsb.db.elasticsearch7.Elasticsearch7.parseIntegerProperty;

/**
 * Elasticsearch REST client for YCSB framework.
 */
public class ElasticsearchRestClient extends DB {

  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9200";
  private static final String DEFAULT_HOST_SCHEMA = "http";
  private static final String CRED_USER_PROPERTY = "es.user";
  private static final String DEFAULT_CRED_USER = "";
  private static final String CRED_PASS_PROPERTY = "es.password";
  private static final String DEFAULT_CRED_PASS = "";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private static final boolean INDEX_QUERY_CACHE_ENABLED_DEFAULT = true;
  private static final String INDEX_QUERY_CACHE_ENABLED_PROPERTY = "es.queries.cache.enabled";
  private static final String CONTENT_TYPE_APPLICATION_JSON = ContentType.APPLICATION_JSON.toString();
  private RestClient restClient;
  private String indexKey;
  private volatile boolean isRefreshNeeded = false;

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
    return performRequest(restClient, method, endpoint, params, null, null);
  }

  private static Response performRequest(
      final RestClient restClient,
      final String method,
      final String endpoint,
      final Map<String, String> params,
      final HttpEntity entity, NStringEntity contentTypeEntity) throws DBException {
    try {
      Request request = new Request(method,
          endpoint);
      request.addParameters(params);
      if (entity != null) {
        request.setEntity(entity);
      }
      return restClient.performRequest(request);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);

    final int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    final int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);
    final boolean queryCacheEnabled = Boolean.parseBoolean(props.getProperty(INDEX_QUERY_CACHE_ENABLED_PROPERTY,
        String.valueOf(INDEX_QUERY_CACHE_ENABLED_DEFAULT)));

    final Boolean newIndex = Boolean.parseBoolean(props.getProperty("es.new_index", "false"));

    final String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
    final String hostSchema = props.getProperty("es.hosts.schema", DEFAULT_HOST_SCHEMA);
    final String user = props.getProperty(CRED_USER_PROPERTY, DEFAULT_CRED_USER);
    final String pass = props.getProperty(CRED_PASS_PROPERTY, DEFAULT_CRED_PASS);
    final CredentialsProvider credentialsProvider =
        new BasicCredentialsProvider();
    if (user!=DEFAULT_CRED_USER && pass != DEFAULT_CRED_PASS) {
      credentialsProvider.setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(user, pass));
    }


    final List<HttpHost> esHttpHosts = new ArrayList<>(nodeList.length);
    for (String h : nodeList) {
      String[] nodes = h.split(":");
      HttpHost httpHost = new HttpHost(nodes[0], Integer.valueOf(nodes[1]), hostSchema);
      esHttpHosts.add(httpHost);
    }

    RestClientBuilder restClientBuilder = RestClient.builder(esHttpHosts.toArray(new HttpHost[esHttpHosts.size()]));
    // Based uppon https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_basic_authentication.html
    if (user!=DEFAULT_CRED_USER && pass != DEFAULT_CRED_PASS) {
      restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
          .setDefaultCredentialsProvider(credentialsProvider));
    }
    restClient = restClientBuilder.build();

    final Response existsResponse = performRequest(restClient,
        "HEAD", "/" + indexKey);
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
        // {
        builder.startObject("mappings");
        //   {
        builder.startObject("properties");
        //     {
        builder.startObject("productName");
        //       {
        builder.field("type", "text");
        //       }
        builder.endObject();
        builder.startObject("productScore");
        //       {
        builder.field("type", "double");
        //       }
        builder.endObject();
        for (String fieldName :
            props.getProperty(CommerceWorkload.NON_INDEXED_FIELDS_SEARCH_PROPERTY,
                CommerceWorkload.NON_INDEXED_FIELDS_SEARCH_PROPERTY_DEFAULT).split(",")
        ) {
          builder.startObject(fieldName);
          //       {
          builder.field("index", "false");
          builder.field("type", "text");
          //       }
          builder.endObject();
        }
        //     }
        builder.endObject();
        //   }
        builder.endObject();
        builder.startObject("settings");
        //   {
        builder.field("index.number_of_shards", numberOfShards);
        builder.field("index.number_of_replicas", numberOfReplicas);
        builder.field("index.refresh_interval", "-1");
        builder.field("index.queries.cache.enabled", queryCacheEnabled);
        //   }
        builder.endObject();
        // }
        builder.endObject();
        final Map<String, String> params = emptyMap();
        final StringEntity entity = new StringEntity(builder.string());
        entity.setContentType(CONTENT_TYPE_APPLICATION_JSON);
        final Response createResponse = performRequest(restClient, "PUT", "/" + indexKey,
            params, entity, null);
        final int statusCode = createResponse.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
          throw new DBException("create [" + indexKey + "] failed with status [" + statusCode + "]");
        }
      } catch (final IOException e) {
        throw new DBException(e);
      }
    }

    final Map<String, String> params = Collections.singletonMap("wait_for_status", "green");
    final Response healthResponse = performRequest(restClient, "GET", "/_cluster/health/" + indexKey,
        params, null, null);
    final int healthStatusCode = healthResponse.getStatusLine().getStatusCode();
    if (healthStatusCode != HttpStatus.SC_OK) {
      throw new DBException("cluster health [" + indexKey + "] failed with status [" + healthStatusCode + "]");
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

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      final Map<String, String> data = StringByteIterator.getStringMap(values);
      data.put(KEY, key);
      NStringEntity payload = new NStringEntity(new ObjectMapper().writeValueAsString(data));
      payload.setContentType(CONTENT_TYPE_APPLICATION_JSON);
      // Reason why we use auto-generated ids and not the specific key id
      // https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-indexing-speed.html
      // When indexing a document that has an explicit id, Elasticsearch needs to check whether a document with the
      // same id already exists within the same shard, which is a costly operation and gets even more costly as
      // the index grows. By using auto-generated ids, Elasticsearch can skip this check, which makes indexing faster.
      final Response response = performRequest(restClient, "POST",
          "/" + indexKey + "/_doc",
          Collections.emptyMap(),
          payload,
          null
      );

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
      @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
      final int total = getTotalHits(hits);
      if (total == 0) {
        return Status.NOT_FOUND;
      }
      @SuppressWarnings("unchecked") final Map<String, Object> hit =
          (Map<String, Object>) ((List<Object>) hits.get("hits")).get(0);
      String hitIdStr = (String) hit.get("_id");
      final Response deleteResponse = performRequest(restClient, "DELETE",
          "/" + indexKey + "/" + hitIdStr);
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
      @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
      final int total = getTotalHits(hits);
      if (total == 0) {
        return Status.NOT_FOUND;
      }
      @SuppressWarnings("unchecked") final Map<String, Object> hit =
          (Map<String, Object>) ((List<Object>) hits.get("hits")).get(0);
      @SuppressWarnings("unchecked") final Map<String, Object> source = (Map<String, Object>) hit.get("_source");
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

  private int getTotalHits(Map<String, Object> hits) {
    @SuppressWarnings("unchecked") final Map<String, Object> hitsTotal = (Map<String, Object>) hits.get("total");
    final int total = (int) hitsTotal.get("value");
    return total;
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
      final int total = getTotalHits(hits);
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
      NStringEntity payload = new NStringEntity(new ObjectMapper().writeValueAsString(source));
      payload.setContentType(CONTENT_TYPE_APPLICATION_JSON);
      final Response response = performRequest(restClient, "PUT",
          "/" + indexKey + "/_doc/" + hit.get("_id"),
          params,
          payload, null
      );
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
        processSearchReply(fields, result, response);
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void processSearchReply(Set<String> fields, Vector<HashMap<String, ByteIterator>> result, Response response)
      throws IOException {
    @SuppressWarnings("unchecked") final Map<String, Object> map = map(response);
    @SuppressWarnings("unchecked") final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
    @SuppressWarnings("unchecked") final List<Map<String, Object>> list =
        (List<Map<String, Object>>) hits.get("hits");

    for (final Map<String, Object> hit : list) {
      @SuppressWarnings("unchecked") final Map<String, Object> source = (Map<String, Object>) hit.get("_source");
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

  @Override
  public Status search(String table,
                       Pair<String, String> queryPair, boolean onlyinsale,
                       Pair<Integer, Integer> pagePair,
                       HashSet<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
    try {
      final Response response;
//     {
//  "query": {
//    "bool": {
//      "must": [{"term":{"productName":"feltcraft"} }, {"term":{"productName":"blue"} }]    }
//  },
//  "sort": [ {"productScore":{}} ],"size": 0,  "track_total_hits": true
//}

      try (XContentBuilder builder = jsonBuilder()) {
        builder.startObject();
        builder.startObject("query");
        builder.startObject("bool");
        builder.startArray("must");
        builder.startObject();
        builder.startObject("term");
        builder.field("productName", queryPair.getValue1().split(" ")[0]);
        builder.endObject();
        builder.endObject();
        builder.startObject();
        builder.startObject("term");
        builder.field("productName", queryPair.getValue1().split(" ")[1]);
        builder.endObject();
        builder.endObject();
        builder.endArray();
        builder.endObject();
        builder.endObject();
        // sorting
        builder.startArray("sort");
        builder.startObject();
        builder.startObject("productScore");
        builder.field("order", "asc");
        builder.endObject();
        builder.endObject();
        builder.endArray();
        builder.field("size", pagePair.getValue1());
        builder.field("track_total_hits", true);
        builder.endObject();
        response = search(table, builder);
        processSearchReply(fields, result, response);
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private void refreshIfNeeded() throws IOException, DBException {
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
        performRequest(restClient, "POST", "/" + indexKey + "/_refresh");
      }
    }
  }

  private Response search(final String table, final String key) throws IOException, DBException {
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

  private Response search(final String table, final XContentBuilder builder) throws IOException, DBException {
    refreshIfNeeded();
    final Map<String, String> params = emptyMap();
    final StringEntity entity = new StringEntity(builder.string());
    entity.setContentType(CONTENT_TYPE_APPLICATION_JSON);
    return performRequest(restClient, "GET", "/" + indexKey + "/_search?request_cache=false",
        params, entity, null);
  }

  private Map<String, Object> map(final Response response) throws IOException {
    try (InputStream is = response.getEntity().getContent()) {
      final ObjectMapper mapper = new ObjectMapper();
      @SuppressWarnings("unchecked") final Map<String, Object> map = mapper.readValue(is, Map.class);
      return map;
    }
  }

}
