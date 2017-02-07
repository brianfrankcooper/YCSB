/**
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

import com.yahoo.ycsb.*;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.settings.Settings.Builder;

/**
 * Elasticsearch REST client for YCSB framework.
 */
public class ElasticsearchRestClient extends DB {

  private static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9200";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private String indexKey;
  private RestClient restClient;
  
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();

    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);

    int numberOfShards = Integer.valueOf(props.getProperty("es.number_of_shards",
        String.valueOf(NUMBER_OF_SHARDS)));
    int numberOfReplicas = Integer.valueOf(props.getProperty("es.number_of_replicas",
        String.valueOf(NUMBER_OF_REPLICAS)));

    Boolean newdb = Boolean.parseBoolean(props.getProperty("es.newdb", "false"));
    Builder settings = Settings.builder().put("cluster.name", DEFAULT_CLUSTER_NAME);

    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    settings.put(props);
    final String clusterName = settings.get("cluster.name");
    System.err.println("Elasticsearch starting node = " + clusterName);

    String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
    System.out.println("Elasticsearch Remote Hosts = " + props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST));

    List<HttpHost> esHttpHosts = new ArrayList<>(nodeList.length);
    for (String h : nodeList) {
      String[] nodes = h.split(":");
      esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "http"));
    }

    restClient = RestClient.builder(esHttpHosts.toArray(new HttpHost[esHttpHosts.size()])).build();

//    final boolean exists =
//        client.admin().indices()
//            .exists(Requests.indicesExistsRequest(indexKey)).actionGet()
//            .isExists();
//    if (exists && newdb) {
//      client.admin().indices().prepareDelete(indexKey).execute().actionGet();
//    }
//    if (!exists || newdb) {
//      client.admin().indices().create(
//          new CreateIndexRequest(indexKey)
//              .settings(
//                  Settings.builder()
//                      .put("index.number_of_shards", numberOfShards)
//                      .put("index.number_of_replicas", numberOfReplicas)
//              )).actionGet();
//    }
//    client.admin().cluster().health(new ClusterHealthRequest().waitForGreenStatus()).actionGet();
  }

  @Override
  public void cleanup() throws DBException {
    if (restClient != null) {
      try {
        restClient.close();
        restClient = null;
      } catch (IOException e) {
        throw new DBException(e);
      }
    }
  }
  
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      Map<String, String> data = StringByteIterator.getStringMap(values);

      Response response = restClient.performRequest(
          HttpPut.METHOD_NAME,
          "/" + indexKey + "/" + table + "/",
          Collections.<String, String>emptyMap(),
          new NStringEntity(new ObjectMapper().writeValueAsString(data), ContentType.APPLICATION_JSON));

      if(response.getStatusLine().getStatusCode() == 200) {
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      Response response = restClient.performRequest(
          HttpDelete.METHOD_NAME,
          "/" + indexKey + "/" + table + "/" + key);

      if(response.getStatusLine().getStatusCode() == 200) {
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      Response response = restClient.performRequest(HttpGet.METHOD_NAME, "/");

      if(response.getStatusLine().getStatusCode() == 200) {
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;

//    try {
//      final GetResponse response = client.prepareGet(indexKey, table, key).execute().actionGet();
//
//      if (response.isExists()) {
//        if (fields != null) {
//          for (String field : fields) {
//            result.put(field, new StringByteIterator(
//                (String) response.getSource().get(field)));
//          }
//        } else {
//          for (String field : response.getSource().keySet()) {
//            result.put(field, new StringByteIterator(
//                (String) response.getSource().get(field)));
//          }
//        }
//        return Status.OK;
//      } else {
//        return Status.NOT_FOUND;
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      return Status.ERROR;
//    }
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
//    try {
//      final GetResponse response = client.prepareGet(indexKey, table, key).execute().actionGet();
//
//      if (response.isExists()) {
//        for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
//          response.getSource().put(entry.getKey(), entry.getValue());
//        }
//
//        client.prepareIndex(indexKey, table, key).setSource(response.getSource()).execute().actionGet();
//
//        return Status.OK;
//      } else {
//        return Status.NOT_FOUND;
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//      return Status.ERROR;
//    }
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
