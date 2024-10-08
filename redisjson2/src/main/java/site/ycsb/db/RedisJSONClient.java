/**
 * Copyright (c) 2021 YCSB contributors. All rights reserved.
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
 * <p>
 * RediSearch client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.
 * For scanning we use RediSearch's secondary index capabilities.
 */

package site.ycsb.db;

import com.google.gson.Gson;
import org.javatuples.Pair;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;
import site.ycsb.*;
import site.ycsb.workloads.CommerceWorkload;
import site.ycsb.workloads.CoreWorkload;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * YCSB binding for <a href="https://github.com/RediSearch/RediSearch/">RediSearch</a>.
 * <p>
 * See {@code redisearch/README.md} for details.
 */
public class RedisJSONClient extends DB {

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String CREATE_FT_INDEX_PROPERTY = "create.search.index";
  public static final String CLUSTER_PROPERTY_DEFAULT = "false";
  public static final String CREATE_FT_INDEX_PROPERTY_DEFAULT = "true";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String CLIENT_POOL_MAX_PROPERTY = "redis.client.poolmaxsize";
  public static final String CLIENT_POOL_MAX_PROPERTY_DEFAULT = "1";
  public static final String INDEX_NAME_PROPERTY = "redis.indexname";
  public static final String INDEX_NAME_PROPERTY_DEFAULT = "index";
  public static final String RANGE_FIELD_NAME_PROPERTY = "redis.scorefield";
  public static final String RANGE_FIELD_NAME_PROPERTY_DEFAULT = "__doc_hash__";
  public static final String INDEXED_TAG_FIELDS_PROPERTY = "redis.indexedtagfields";
  public static final String INDEXED_TAG_FIELDS_PROPERTY_DEFAULT = "";
  public static final String INDEXED_TEXT_FIELDS_PROPERTY = "redis.indexedtextfields";
  public static final String INDEXED_TEXT_FIELDS_PROPERTY_DEFAULT = "productName";
  public static final String RESULT_PROCESS_PROPERTY = "redis.enable.resultprocess";
  public static final String RESULT_PROCESS_PROPERTY_DEFAULT = "true";
  public static final String ROOT_PATH_PROPERTY = "json.root";
  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  public static final String ROOT_PATH_PROPERTY_DEFAULT = "$";
  private static final Gson GSON = new Gson();

  private static final boolean INDEX_JSON_ENABLED_PROPERTY_DEFAULT = true;
  private static final String INDEX_JSON_ENABLED_PROPERTY = "redisjson.indexdocs";

  private static JedisCluster jedisCluster;
  private static JedisPool jedisPool;
  private Boolean clusterEnabled;
  private Boolean createFTIndex;
  private int fieldCount;
  private String indexName;
  private String rangeField;
  private String rootPath;
  private boolean orderedinserts;
  private boolean coreWorkload;
  private String keyprefix;
  private Set<String> commerceTagFields;
  private Set<String> commerceTextFields;
  private Set<String> nonIndexFields;
  private boolean resultProcessing;

  /*
   * @Description: Method to convert Map to JSON String
   * @param: map Map<String, String>
   * @return: json String
   */
  public static String convert(Map<String, String> map) {
    String json = GSON.toJson(map);
    return json;
  }


  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    Properties props = getProperties();
    int port = Protocol.DEFAULT_PORT;
    String host = Protocol.DEFAULT_HOST;
    int timeout = Protocol.DEFAULT_TIMEOUT;

    final boolean indexingJsonEnabled = Boolean.parseBoolean(props.getProperty(INDEX_JSON_ENABLED_PROPERTY,
        String.valueOf(INDEX_JSON_ENABLED_PROPERTY_DEFAULT)));

    String redisTimeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY, CLUSTER_PROPERTY_DEFAULT));
    createFTIndex = Boolean.parseBoolean(props.getProperty(CREATE_FT_INDEX_PROPERTY, CREATE_FT_INDEX_PROPERTY_DEFAULT));
    resultProcessing = Boolean.parseBoolean(props.getProperty(RESULT_PROCESS_PROPERTY,
        RESULT_PROCESS_PROPERTY_DEFAULT));
    String portString = props.getProperty(PORT_PROPERTY);
    indexName = props.getProperty(INDEX_NAME_PROPERTY, INDEX_NAME_PROPERTY_DEFAULT);
    rootPath = props.getProperty(ROOT_PATH_PROPERTY, ROOT_PATH_PROPERTY_DEFAULT);
    rangeField = props.getProperty(RANGE_FIELD_NAME_PROPERTY, RANGE_FIELD_NAME_PROPERTY_DEFAULT);


    keyprefix = "user";
    if (portString != null) {
      port = Integer.parseInt(portString);
    }
    orderedinserts = props.getProperty(CoreWorkload.INSERT_ORDER_PROPERTY,
        CommerceWorkload.INSERT_ORDER_PROPERTY_DEFAULT).compareTo("ordered") == 0;
    if (props.getProperty(HOST_PROPERTY) != null) {
      host = props.getProperty(HOST_PROPERTY);
    }
    if (redisTimeoutStr != null) {
      timeout = Integer.parseInt(redisTimeoutStr);
    }

    if (jedisPool != null) {
      return;
    }

    JedisPoolConfig poolConfig = new JedisPoolConfig();

    //get number of threads, and set it to the pool max size
    int poolMaxTotal = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    poolConfig.setMaxTotal(poolMaxTotal);
    if (clusterEnabled) {
      Set<HostAndPort> startNodes = new HashSet<>();
      jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
      List<Object> clusterNodes = jedisPool.getResource().clusterSlots();
      for (Object slotDetail : clusterNodes
      ) {
        List<Object> nodeDetail = (List<Object>) ((List<Object>) slotDetail).get(2);
        String h = new String((byte[]) nodeDetail.get(0));
        long p = (long) nodeDetail.get(1);
        System.err.println(h + " : " + p);
        startNodes.add(new HostAndPort(h, (int) p));
      }
      jedisCluster = new JedisCluster(startNodes, timeout, timeout, 5, password, poolConfig);
    } else {
      jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
    }

    fieldCount = Integer.parseInt(props.getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    if (indexingJsonEnabled) {
      try (Jedis setupPoolConn = getResource()) {
        coreWorkload = props.getProperty("workload").compareTo("site.ycsb.workloads.CoreWorkload") == 0;
        if (coreWorkload) {
          setupPoolConn.sendCommand(RediSearchCommands.CREATE,
              coreWorkloadIndexCreateCmdArgs(indexName).toArray(String[]::new));
        } else {
          commerceTagFields = new TreeSet<>();
          commerceTextFields = new TreeSet<>();
          nonIndexFields = new TreeSet<>();
          String[] tagFields = props.getProperty(INDEXED_TAG_FIELDS_PROPERTY,
              INDEXED_TAG_FIELDS_PROPERTY_DEFAULT).split(",");
          if (tagFields.length > 0) {
            for (String tagFieldName : tagFields
            ) {
              commerceTagFields.add(tagFieldName);
            }
          }
          for (String textFieldName :
              props.getProperty(INDEXED_TEXT_FIELDS_PROPERTY, INDEXED_TEXT_FIELDS_PROPERTY_DEFAULT).split(",")
          ) {
            commerceTextFields.add(textFieldName);
          }
          for (String fieldName :
              props.getProperty(CommerceWorkload.NON_INDEXED_FIELDS_SEARCH_PROPERTY,
                  CommerceWorkload.NON_INDEXED_FIELDS_SEARCH_PROPERTY_DEFAULT).split(",")
          ) {
            nonIndexFields.add(fieldName);
          }
          if (createFTIndex) {
            setupPoolConn.sendCommand(RediSearchCommands.CREATE,
                commerceWorkloadIndexCreateCmdArgs(indexName).toArray(String[]::new));
          }
        }
      } catch (redis.clients.jedis.exceptions.JedisDataException e) {
        if (!e.getMessage().contains("Index already exists")) {
          throw new DBException(e.getMessage());
        }
      }
    }

  }

  private List<String> commerceWorkloadIndexCreateCmdArgs(String iName) {
    List<String> args = new ArrayList<>(Arrays.asList(iName,
        "ON", "JSON",
        "NOFIELDS", "NOFREQS", "NOOFFSETS",
        "SCHEMA",
        "$.productScore", "AS", "productScore", "NUMERIC", "SORTABLE", "NOINDEX",
        "$.productName", "AS", "productName", "TEXT", "NOSTEM", "SORTABLE"));

    return args;
  }

  private Jedis getResource() {
    if (clusterEnabled) {
      return jedisCluster.getConnectionFromSlot(ThreadLocalRandom.current()
          .nextInt(JedisCluster.HASHSLOTS));
    } else {
      return jedisPool.getResource();
    }
  }

  private Jedis getResource(String key) {
    if (clusterEnabled) {
      return jedisCluster.getConnectionFromSlot(JedisClusterCRC16.getSlot(key));
    } else {
      return jedisPool.getResource();
    }
  }

  /**
   * Helper method to create the FT.CREATE command arguments, used to add a secondary index definition to Redis.
   *
   * @param iName Index name
   * @return
   */
  private List<String> coreWorkloadIndexCreateCmdArgs(String iName) {
    List<String> args = new ArrayList<>(Arrays.asList(iName, "ON", "JSON",
        "SCHEMA", rangeField, "NUMERIC", "SORTABLE"));
    return args;
  }

  @Override
  public void cleanup() throws DBException {
//    if (jedisPool != null) {
//      return;
//    }
//    try {
//      if (clusterEnabled) {
//        jedisCluster.close();
//      } else {
//        jedisPool.close();
//      }
//    } catch (Exception e) {
//      throw new DBException("Closing connection failed.", e);
//    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of int's. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private int hash(String key) {
    if (orderedinserts) {
      return Integer.parseInt(key.replaceAll(keyprefix, ""));
    } else {
      return key.hashCode();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Status res = Status.OK;
    try (Jedis j = getResource(key)) {
      if (fields == null) {
        j.getClient().sendCommand(RedisJSONCommands.GET, key);
      } else {
        ArrayList<String> jsonGetCommandArgs = new ArrayList<>(Arrays.asList(key));
        for (String field : fields) {
          jsonGetCommandArgs.add("$." + field);
        }
        j.getClient().sendCommand(RedisJSONCommands.GET, jsonGetCommandArgs.toArray(String[]::new));
      }
      String rep = j.getClient().getBulkReply();
      CommerceDocument obj = GSON.fromJson(rep, CommerceDocument.class);
      result = obj.getFieldMap();
    } catch (Exception e) {
      res = Status.ERROR;
    }
    return res;
  }


  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    if (coreWorkload) {
      values.put(rangeField, new StringByteIterator(String.valueOf(hash(key))));
    }
    try (Jedis j = getResource(key)) {
      String pss = String.valueOf(values.get("productScore"));
      values.remove("productScore");
      String partialJson = convert(StringByteIterator.getStringMap(values));
      partialJson = "{\"productScore\": " + pss + " , " + partialJson.substring(1);
      j.getClient().sendCommand(RedisJSONCommands.SET, key, rootPath, partialJson);
      if (j.getClient().getStatusCodeReply().compareTo("OK") == 0) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try (Jedis j = getResource(key)) {
      j.del(key);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try (Jedis j = getResource(key)) {
      ArrayList<String> jsonGetCommandArgs = new ArrayList<>(Arrays.asList(key));
      // using iterators
      Iterator<Map.Entry<String, ByteIterator>> itr = values.entrySet().iterator();
      Gson gson = new Gson();
      while (itr.hasNext()) {
        Map.Entry<String, ByteIterator> entry = itr.next();
        jsonGetCommandArgs.add("." + entry.getKey());
        jsonGetCommandArgs.add("\"" + String.valueOf(entry.getValue()) + "\"");
      }
      j.getClient().sendCommand(RedisJSONCommands.SET, jsonGetCommandArgs.toArray(String[]::new));
      if (j.getClient().getStatusCodeReply().compareTo("OK") == 0) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status search(String table,
                       Pair<String, String> queryPair, boolean onlyinsale,
                       Pair<Integer, Integer> pagePair,
                       HashSet<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {

    List<Object> resp;
    try (Jedis j = getResource()) {
      resp = (List<Object>) j.sendCommand(RediSearchCommands.SEARCH,
          searchCommandArgs(indexName, queryPair, onlyinsale, pagePair, fields));
      if (resultProcessing) {
//        long totalResult = (long) resp.get(0);
        for (int i = 1; i < resp.size(); i += 2) {
          String keyname = SafeEncoder.encode((byte[]) resp.get(i));
          List<byte[]> docDetails = (List<byte[]>) resp.get(i + 1);
          // Check if the reply includes score field
          int docStartPos = 0;
          if (docDetails.size() >= 4) {
            docStartPos = 2;
          }
          String rep = SafeEncoder.encode(docDetails.get(docStartPos + 1));
          CommerceDocument obj = GSON.fromJson(rep, CommerceDocument.class);
          result.add(obj.getFieldMap());
        }
      }
    } catch (Exception e) {
      throw e;
    }
    return Status.OK;
  }


  private String[] searchCommandArgs(String idxName, Pair<String, String> queryPair, boolean onlyinsale,
                                     Pair<Integer, Integer> pagePair, HashSet<String> rFields) {
    int returnFieldsCount = fieldCount;
    if (rFields != null) {
      returnFieldsCount = rFields.size();
    }
    String fieldName = queryPair.getValue0();
    String query;
    if (commerceTextFields.contains(fieldName)) {
      String[] words = queryPair.getValue1().split(" ");
      query = words[0] + " " + words[1];
    } else {
      String tagValue = queryPair.getValue1().replaceAll(" ", "\\\\ ");
      query = String.format("@%s:{%s}", fieldName, tagValue);
    }

    ArrayList<String> searchCommandArgs = new ArrayList<>(Arrays.asList(idxName,
        query,
        "VERBATIM",
        "SORTBY", "productScore",
        "LIMIT", String.valueOf(pagePair.getValue0()),
        String.valueOf(pagePair.getValue0() + pagePair.getValue1() - 1)));
    if (rFields != null) {
      searchCommandArgs.addAll(Arrays.asList("RETURN", String.valueOf(returnFieldsCount)));
      for (String field : rFields) {
        searchCommandArgs.add(field);
      }
    }
    return searchCommandArgs.toArray(String[]::new);
  }

  /**
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * RediSearch Protocol commands.
   */
  public enum RediSearchCommands implements ProtocolCommand {

    CREATE,
    SEARCH;

    private final byte[] raw;

    RediSearchCommands() {
      this.raw = SafeEncoder.encode("FT." + name());
    }

    @Override
    public byte[] getRaw() {
      return this.raw;
    }
  }

  /**
   * RedisJSON Protocol commands.
   */
  public enum RedisJSONCommands implements ProtocolCommand {

    SET,
    GET;

    private final byte[] raw;

    RedisJSONCommands() {
      this.raw = SafeEncoder.encode("JSON." + name());
    }

    @Override
    public byte[] getRaw() {
      return this.raw;
    }
  }
}
