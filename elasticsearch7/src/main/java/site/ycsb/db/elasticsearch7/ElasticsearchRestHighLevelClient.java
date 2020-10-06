/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static site.ycsb.db.elasticsearch7.Elasticsearch7.KEY;
import static site.ycsb.db.elasticsearch7.Elasticsearch7.parseIntegerProperty;
import static site.ycsb.db.elasticsearch7.Elasticsearch7.parseStringProperty;

/**
 * Elasticsearch 7.x Java High Level REST Client for YCSB framework.
 * @author xosk31
 * 
 * <p>
 * See {@code README.md} to configure and view more details.
 * </p>
 * 
 * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client
 *         /java-rest/master/java-rest-high.html">Elastic.co</a>
 *
 */

@SuppressWarnings("deprecation")
public class ElasticsearchRestHighLevelClient extends DB {

  private RestHighLevelClient client;
  private static ElasticsearchRestHighLevelClient instance;
  private ClusterHealthStatus clusterState;
  private boolean existsIndex;
  
  private Logger logger = Logger.getLogger("ElasticsearchRestHighLevelClient");

  public static ElasticsearchRestHighLevelClient getInstance() {
    if (instance == null) {
      instance = new ElasticsearchRestHighLevelClient();
    }
    return instance;
  }

  private static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
  private static final String DEFAULT_INDEX_NAME = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "0.0.0.0:9200";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  
  private String keyName;
  private String indexName;
  
  private static final String DEFAULT_SECURITY_SSL = "false";
  private static final String DEFAULT_SECURITY_SSL_PATH = "/root/certificates.p12";
  private static final String DEFAULT_AUTHENTICATION = "false";
  private static final String DEFAULT_CREDENTIALS_USER = "elastic";
  private static final String DEFAULT_CREDENTIALS_PASSWORD = "changeme"; 

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    final Properties props = getProperties();
    
    /** General properties */
    final String clusterName = parseStringProperty(props, "es.cluster.name", DEFAULT_CLUSTER_NAME);
    keyName = parseStringProperty(props, "es.index.key", KEY);
    indexName = parseStringProperty(props, "es.index.name", DEFAULT_INDEX_NAME);
    final int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    final int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);
    final String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
    final List<HttpHost> esHttpHosts = new ArrayList<HttpHost>(nodeList.length);
    
    /** Parameters to configure SSL and Authentication */
    final String securitySSL = parseStringProperty(props, "es.security.ssl", DEFAULT_SECURITY_SSL);
    boolean boolSecuritySSL = Boolean.parseBoolean(securitySSL);  
    final String securitySSLPath = parseStringProperty(props, "es.security.ssl.path", DEFAULT_SECURITY_SSL_PATH);
    final String elasticAuthentication = parseStringProperty(props, "es.authentication", DEFAULT_AUTHENTICATION);
    boolean boolElasticAuthentication = Boolean.parseBoolean(elasticAuthentication); 
    final String elasticUser = parseStringProperty(props, "es.credentials.user", DEFAULT_CREDENTIALS_USER);
    final String elasticPassword = parseStringProperty(props, "es.credentials.password", DEFAULT_CREDENTIALS_PASSWORD);

    logger.info("You configure the benchmark with this properties: " + props);
    logger.info("Creating elasticsearch client");
    
    /** Check security and connect to the cluster */
    if(boolSecuritySSL && boolElasticAuthentication) {
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "https"));
      }
      client = connectSSLAndAuthentication(clusterName, securitySSLPath, elasticUser, elasticPassword, esHttpHosts);
    }else if (boolElasticAuthentication && !boolSecuritySSL) {
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "http"));
      }
      client = connectAuthentication(clusterName, elasticUser, elasticPassword, esHttpHosts);
    }else if (boolSecuritySSL && !boolElasticAuthentication) {
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "https"));
      }
      client = connectSSL(clusterName, securitySSLPath, esHttpHosts);
    }else {
      for (String h : nodeList) {
        String[] nodes = h.split(":");
        esHttpHosts.add(new HttpHost(nodes[0], Integer.valueOf(nodes[1]), "http"));
      }
      client = connect(clusterName, esHttpHosts);
    }    

    /** Verify the health of the cluster */
    ClusterHealthRequest requestHealth = new ClusterHealthRequest();
    ClusterHealthResponse response = null;
 
    try {
      response = client.cluster().health(requestHealth, RequestOptions.DEFAULT);
      logger.info("The properties of the cluster is " + response);
      clusterState = response.getStatus();
      logger.info("The state of the cluster is " + clusterState);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (clusterState.equals(ClusterHealthStatus.RED)) {
      try {
        logger.info("The conection is imposible because the cluster state is " + clusterState);
        client.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }else{
      GetIndexRequest requestExist = new GetIndexRequest(indexName);
      try {
        existsIndex = client.indices().exists(requestExist, RequestOptions.DEFAULT);
        logger.info("The " + indexName + " index exists: " + existsIndex);
      } catch (IOException e) {
        e.printStackTrace();
      }

      if (existsIndex) {
        try {
          GetIndexResponse getIndexResponse = client.indices().get(requestExist, RequestOptions.DEFAULT);
          logger.info("The settings of the index " + indexName + " is: " + getIndexResponse.getSettings());
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        CreateIndexRequest requestCreate = new CreateIndexRequest(indexName);
        requestCreate.settings(Settings.builder() 
            .put("index.number_of_shards", numberOfShards)
            .put("index.number_of_replicas", numberOfReplicas));
        try {
          CreateIndexResponse createIndexResponse = client.indices().create(requestCreate, RequestOptions.DEFAULT);
          logger.info("The index " + indexName + " is created: " + createIndexResponse.isAcknowledged());
          GetIndexRequest requestNewIndex = new GetIndexRequest(indexName);
          GetIndexResponse getIndexResponse = client.indices().get(requestNewIndex, RequestOptions.DEFAULT);
          logger.info("The settings of the index " + indexName + "is: " + getIndexResponse.getSettings());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      try {
        client.close();  
      } catch (final IOException e) {
        throw new DBException(e);
      } finally {
        client = null;  
      }
    }
  }
  
  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a
   * HashMap.
   *
   * @param table
   *          Not used because Elastic in this version removes the parameter type. 
   *          Parameter table is named _doc in Elasticsearch.
   * @param key
   *          The record key of the record to read.
   *          In Elasticsearch, the key corresponds to _id field.          
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    GetRequest getRequest = new GetRequest(indexName, key); 
    GetResponse getResponse = null;

    try {
      getResponse = client.get(getRequest, RequestOptions.DEFAULT);    
      if (fields != null) {
        for (String field : fields) {
          if (getResponse.getSource().get(field) == null) {
            return Status.NOT_FOUND;
          } else{
            result.put(field, new StringByteIterator((String)getResponse.getSource().get(field)));
          }
        }
      }
      //logger.info("The document is read " + Status.OK);
      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }    
  }
  
  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          Not used because Elastic in this version removes the parameter type. 
   *          Parameter table is named _doc in Elasticsearch.
   * @param startkey
   *          The record key of the first record to read.
   *          In Elasticsearch, the key corresponds to _id field.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error or "not found". See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    
    SearchRequest searchRequest = new SearchRequest(indexName);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.rangeQuery(keyName).gte(startkey)).size(recordcount);
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = null;

    try {
      searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      //logger.info("The number of documents which are scaned is " + searchResponse.getHits().getHits().length);
    } catch (IOException e) {
      //logger.info("An error occurred during the search process");
      e.printStackTrace();
      return Status.ERROR;
    }
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();
    if (searchHits.length == 0) {
      logger.info("The document with the _id: " + keyName + "does not exists");
      return Status.NOT_FOUND;
    } else {
      HashMap<String, ByteIterator> entry;
      if (fields != null) {
        for (SearchHit hit : searchHits) {
          //logger.info("The document with the id: " + hit.getId() + " is scanned.");
          entry = new HashMap<String, ByteIterator>(fields.size());
          for (String field : fields) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String document = (String) sourceAsMap.get(field);
            entry.put(field, new StringByteIterator(document));
          }
          result.add(entry);
        }
      }else {
        for (SearchHit hit : searchHits) {
          //logger.info("The document with the id: " + hit.getId() + " is scanned.");
          entry = new HashMap<String, ByteIterator>();
          Map<String, Object> sourceAsMap = hit.getSourceAsMap();
          for (Entry<String, Object> entry2 : sourceAsMap.entrySet()) {
            String document = (String) sourceAsMap.get(entry2.getValue());
            entry.put(entry2.getKey(), new StringByteIterator(document));
          }  
          result.add(entry);
        }
      }
      //logger.info("The documents are scanned: " + Status.OK);
      return Status.OK;
    }
  }
  
  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          Not used because Elastic in this version removes the parameter type. 
   *          Parameter table is named _doc in Elasticsearch.
   * @param key
   *          The record key of the record to write.
   *          In Elasticsearch, the key corresponds to _id field.
   * @param values
   *          A HashMap of field/value pairs to update in the record.
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      final XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }
      doc.field(keyName, key);
      doc.endObject();
      UpdateRequest request = new UpdateRequest(indexName, key).doc(doc);
      
      try {
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        //logger.info("The document is updated: " + updateResponse.status());
      } catch (IOException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    //logger.info("The document is updated: " + Status.OK);
    return Status.OK;
  }
  
  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
    * @param table
   *          Not used because Elastic in this version removes the parameter type. 
   *          Parameter table is named _doc in Elasticsearch.
   * @param key
   *          The record key of the record to insert.
   *          In Elasticsearch, the key corresponds to _id field.
   * @param values
   *          A HashMap of field/value pairs to insert in the record.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      final XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        doc.field(entry.getKey(), entry.getValue());
      }
      doc.field(keyName, key);
      doc.endObject();
      IndexRequest indexRequest = new IndexRequest(indexName).id(key).source(doc);
      try {
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        //logger.info("The document is inserted: " + indexResponse.status());
      } catch (IOException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    //logger.info("The document is stored: " + Status.OK);
    return Status.OK;
  }
  
  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table.
   *          Parameter table is named _doc in Elasticsearch.
   * @param key
   *          The record key of the record to delete.
   *          In Elasticsearch, the key corresponds to _id field.
   * @return Zero on success, a non-zero error code on error or "not found". See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    SearchRequest searchRequest = new SearchRequest(indexName);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("_id", key)));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = null;

    try {
      searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();
    if (searchHits.length == 0) {
      logger.info("The document with the _id: " + key + " does not exists");
      return Status.NOT_FOUND;
    } else {
      for (SearchHit hit : searchHits) {
        if (key.equals(hit.getId())) {
          //logger.info("The document is: " + hit.getId());
          DeleteRequest request = new DeleteRequest(indexName, key);
          try {
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            //logger.info("The document is deleted: " + deleteResponse.status());
          } catch (IOException e) {
            e.printStackTrace();
            return Status.ERROR;
          }
        }
      }
    }
    //logger.info("The document is deleted: " + Status.OK);
    return Status.OK;
  }
  
  /**
   * Connect to Elasticsearch with SSL certificates and with authentication.
   * This configuration encrypts traffic to, from and within an Elasticsearch cluster using SSL/TLS.
   * This configuration restricts the access to the cluster offering an authentication process to 
   * identify the users behind the requests that hit the cluster. The user must prove their identity,
   * via passwords or credentials. 
   * 
   * @param clusterName
   *           Elasticsearch cluster name.
   * @param securitySSLPath
   *           Path to the certificates.
   * @param elasticUser
   *           User name credentials.
   * @param elasticPassword
   *           User password credentials.
   * @param esHttpHosts
   *           List of hosts of the Elasticsearch cluster.
   * @return Java High Level REST Client: the official high-level client for Elasticsearch.
   */
  private RestHighLevelClient connectSSLAndAuthentication(String clusterName, String securitySSLPath,
           String elasticUser, String elasticPassword, List<HttpHost> esHttpHosts){
    try {
      Settings.Builder settings = Settings.builder();
      settings.put("cluster.name", clusterName);
 
      logger.info("Connecting...");
        
      KeyStore truststore = KeyStore.getInstance("jks");
        
      try (InputStream is = new FileInputStream(securitySSLPath)) {
        truststore.load(is, "".toCharArray());
      }

      SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);
      final SSLContext sslContext = sslBuilder.build();
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUser, elasticPassword));
      
      RestClientBuilder builder = RestClient.builder(esHttpHosts
          .toArray(new HttpHost[esHttpHosts.size()]));               
      builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder
            .setDefaultCredentialsProvider(credentialsProvider)
            .setSSLContext(sslContext)
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
      });

      client = new RestHighLevelClient(builder);
      logger.info("Connected!");
      
    } catch (KeyManagementException | NoSuchAlgorithmException | 
      CertificateException | KeyStoreException | IOException e) {
      e.printStackTrace();
    }
    return client;
  }
  
  /**
   * Connect to Elasticsearch with authentication.
   * This configuration restricts the access to the cluster offering an authentication process to 
   * identify the users behind the requests that hit the cluster. The user must prove their identity,
   * via passwords or credentials.
   * 
   * @param clusterName
   *           Elasticsearch cluster name.
   * @param elasticUser
   *           User name credentials.
   * @param elasticPassword
   *           User password credentials.
   * @param esHttpHosts
   *           List of hosts of the Elasticsearch cluster.
   * @return Java High Level REST Client: the official high-level client for Elasticsearch. 
   */
  private RestHighLevelClient connectAuthentication(String clusterName, String elasticUser, 
         String elasticPassword, List<HttpHost> esHttpHosts) {

    Settings.Builder settings = Settings.builder();
    settings.put("cluster.name", clusterName);
 
    logger.info("Connecting...");

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUser, elasticPassword));
      
    RestClientBuilder builder = RestClient.builder(esHttpHosts
        .toArray(new HttpHost[esHttpHosts.size()]));               
    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder
          .setDefaultCredentialsProvider(credentialsProvider)
          .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      }
    });

    client = new RestHighLevelClient(builder);
    logger.info("Connected!");
    return client;
  }
  
  /**
   * Connect to Elasticsearch with SSL certificates.
   * This configuration encrypts traffic to, from and within an Elasticsearch cluster using SSL/TLS.
   * 
   * @param clusterName
   *           Elasticsearch cluster name.
   * @param securitySSLPath
   *           Path to the certificates.
   * @param esHttpHosts
   *           List of hosts of the Elasticsearch cluster.
   * @return Java High Level REST Client: the official high-level client for Elasticsearch. 
   */
  private RestHighLevelClient connectSSL(String clusterName, String securitySSLPath, List<HttpHost> esHttpHosts) {
    try {
      Settings.Builder settings = Settings.builder();
      settings.put("cluster.name", clusterName);
 
      logger.info("Connecting...");
        
      KeyStore truststore = KeyStore.getInstance("jks");
        
      try (InputStream is = new FileInputStream(securitySSLPath)) {
        truststore.load(is, "".toCharArray());
      }

      SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);
      final SSLContext sslContext = sslBuilder.build();
      
      RestClientBuilder builder = RestClient.builder(esHttpHosts
          .toArray(new HttpHost[esHttpHosts.size()]));               
      builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder
            .setSSLContext(sslContext)
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
      });

      client = new RestHighLevelClient(builder);
      logger.info("Connected!");
      
    } catch (KeyManagementException | NoSuchAlgorithmException | 
      CertificateException | KeyStoreException | IOException e) {
      e.printStackTrace();
    }
    return client;
  }
  
  /**
   * Connect to Elasticsearch without SSL certificates or authentication.
   * 
   * @param clusterName
   *           Elasticsearch cluster name.
   * @param esHttpHosts
   *           List of hosts of the Elasticsearch cluster.
   * @return Java High Level REST Client: the official high-level client for Elasticsearch.
   */
  private RestHighLevelClient connect(String clusterName, List<HttpHost> esHttpHosts) {

    Settings.Builder settings = Settings.builder();
    settings.put("cluster.name", clusterName);
 
    logger.info("Connecting...");
      
    RestClientBuilder builder = RestClient.builder(esHttpHosts.toArray(new HttpHost[esHttpHosts.size()]));

    client = new RestHighLevelClient(builder);
    logger.info("Connected!");
    return client;
  }
  
  /**
   * Java High Level REST Client: the official high-level client for Elasticsearch. 
   * 
   * @return Java High Level REST Client.
   */
  public RestHighLevelClient getTransportClient() {
    return client;
  }

  /**
   * Java-based logging utility.
   * 
   * @return Log4j
   */
  public Logger getLogger() {
    return logger;
  }

  /**
   * Java-based logging utility.
   * 
   * @param loggerAux 
   *           Log4j
   */
  public void setLogger(Logger loggerAux) {
    this.logger = loggerAux;
  }
}
