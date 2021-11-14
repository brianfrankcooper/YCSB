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

/**
 * YCSB binding for <a href="https://github.com/RediSearch/RediSearch/">RediSearch</a>.
 * <p>
 * See {@code redisearch/README.md} for details.
 */
public class RediSearchClient extends DB {
  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String CLUSTER_PROPERTY_DEFAULT = "false";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String CLIENT_POOL_MAX_PROPERTY = "redis.client.poolmaxsize";
  public static final String CLIENT_POOL_MAX_PROPERTY_DEFAULT = "1";
  public static final String INDEX_NAME_PROPERTY = "redisearch.indexname";
  public static final String INDEX_NAME_PROPERTY_DEFAULT = "index";
  public static final String RANGE_FIELD_NAME_PROPERTY = "redisearch.scorefield";
  public static final String RANGE_FIELD_NAME_PROPERTY_DEFAULT = "__doc_hash__";
  public static final String INDEXED_TAG_FIELDS_PROPERTY = "redisearch.indexedtagfields";
  //  public static final String INDEXED_TAG_FIELDS_PROPERTY_DEFAULT = "brand,department,color,inSale,inStock";
  public static final String INDEXED_TAG_FIELDS_PROPERTY_DEFAULT = "";
  public static final String INDEXED_TEXT_FIELDS_PROPERTY = "redisearch.indexedtextfields";
  //  public static final String INDEXED_TEXT_FIELDS_PROPERTY_DEFAULT = "productName,productDescription";
  public static final String INDEXED_TEXT_FIELDS_PROPERTY_DEFAULT = "productName";
  public static final String RESULT_PROCESS_PROPERTY = "redisearch.enable.resultprocess";
  public static final String RESULT_PROCESS_PROPERTY_DEFAULT = "true";
  private static final boolean INDEX_HASHES_ENABLED_PROPERTY_DEFAULT = true;
  private static final String INDEX_HASHES_ENABLED_PROPERTY = "redisearch.indexdocs";
  private JedisCluster jedisCluster;
  private JedisPool jedisPool;
  private Boolean clusterEnabled;
  private int fieldCount;
  private String fieldPrefix;
  private String indexName;
  private String rangeField;
  private boolean orderedinserts;
  private boolean coreWorkload;
  private String keyprefix;
  private Set<String> commerceTagFields;
  private Set<String> commerceTextFields;
  private Set<String> nonIndexFields;
  private boolean resultProcessing;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    int port = Protocol.DEFAULT_PORT;
    String host = Protocol.DEFAULT_HOST;
    int timeout = Protocol.DEFAULT_TIMEOUT;

    final boolean indexingHashesEnabled = Boolean.parseBoolean(props.getProperty(INDEX_HASHES_ENABLED_PROPERTY,
        String.valueOf(INDEX_HASHES_ENABLED_PROPERTY_DEFAULT)));

    String redisTimeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY, CLUSTER_PROPERTY_DEFAULT));
    resultProcessing = Boolean.parseBoolean(props.getProperty(RESULT_PROCESS_PROPERTY,
        RESULT_PROCESS_PROPERTY_DEFAULT));
    String portString = props.getProperty(PORT_PROPERTY);
    indexName = props.getProperty(INDEX_NAME_PROPERTY, INDEX_NAME_PROPERTY_DEFAULT);
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

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    int poolMaxTotal = Integer.parseInt(props.getProperty(CLIENT_POOL_MAX_PROPERTY, CLIENT_POOL_MAX_PROPERTY_DEFAULT));
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
    fieldPrefix = props.getProperty(
        CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

    if (indexingHashesEnabled) {
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
          setupPoolConn.sendCommand(RediSearchCommands.CREATE,
              commerceWorkloadIndexCreateCmdArgs(indexName).toArray(String[]::new));
        }
      } catch (redis.clients.jedis.exceptions.JedisDataException e) {
        if (!e.getMessage().contains("Index already exists")) {
          throw new DBException(e.getMessage());
        }
      }
    }
  }

  private List<String> commerceWorkloadIndexCreateCmdArgs(String iName) {
    List<String> args = new ArrayList<>(Arrays.asList(iName, "ON", "HASH",
        "NOFIELDS", "NOFREQS", "NOOFFSETS",
        "SCHEMA", "productScore", "NUMERIC", "SORTABLE", "NOINDEX"));
    Iterator iterator = commerceTagFields.iterator();
//    while (iterator.hasNext()) {
//      args.addAll(new ArrayList<>(Arrays.asList(iterator.next().toString(), "TAG")));
//    }
    iterator = commerceTextFields.iterator();
    while (iterator.hasNext()) {
      args.addAll(new ArrayList<>(Arrays.asList(iterator.next().toString(), "TEXT", "NOSTEM", "SORTABLE")));
    }
//    iterator = nonIndexFields.iterator();
//    while (iterator.hasNext()) {
//      args.addAll(new ArrayList<>(Arrays.asList(iterator.next().toString(), "TEXT", "NOINDEX")));
//    }
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
    List<String> args = new ArrayList<>(Arrays.asList(iName, "ON", "HASH",
        "SCHEMA", rangeField, "NUMERIC", "SORTABLE"));
    return args;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (clusterEnabled) {
        jedisCluster.close();
      } else {
        jedisPool.close();
      }
    } catch (Exception e) {
      throw new DBException("Closing connection failed.", e);
    }
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
        Map<String, String> reply = j.hgetAll(key);
        extractHGetAllResults(result, reply);
      } else {
        List<String> reply = j.hmget(key, fields.toArray(new String[fields.size()]));
        if (resultProcessing) {
          extractHmGetResults(fields, result, reply);
          res = result.isEmpty() ? Status.NOT_FOUND : Status.OK;
        }
      }
    } catch (Exception e) {
      res = Status.ERROR;
    }
    return res;
  }

  private void extractHGetAllResults(Map<String, ByteIterator> result, Map<String, String> reply) {
    StringByteIterator.putAllAsByteIterators(result, reply);
  }

  private void extractHmGetResults(Set<String> fields, Map<String, ByteIterator> result, List<String> values) {
    Iterator<String> fieldIterator = fields.iterator();
    Iterator<String> valueIterator = values.iterator();

    while (fieldIterator.hasNext() && valueIterator.hasNext()) {
      result.put(fieldIterator.next(),
          new StringByteIterator(valueIterator.next()));
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    if (coreWorkload) {
      values.put(rangeField, new StringByteIterator(String.valueOf(hash(key))));
    }
    try (Jedis j = getResource(key)) {
      j.hset(key, StringByteIterator.getStringMap(values));
      return Status.OK;
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
      j.hset(key, StringByteIterator.getStringMap(values));
      return Status.OK;
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
        processFTSearchReply(result, resp);
      }
    } catch (Exception e) {
      throw e;
    }
    return Status.OK;
  }

  private void processFTSearchReply(Vector<HashMap<String, ByteIterator>> result, List<Object> resp) {
    for (int i = 1; i < resp.size(); i += 2) {
      List<byte[]> docFields = (List<byte[]>) resp.get(i + 1);
      HashMap<String, ByteIterator> values = new HashMap<>();
      for (int k = 2; k < docFields.size(); k += 2) {
        values.put(SafeEncoder.encode(docFields.get(k)),
            new StringByteIterator(SafeEncoder.encode(docFields.get(k + 1))));
        result.add(values);
      }
    }
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
   * As you will see below, there is the need to model a scan operation within different records,
   * in which we scan records in order, starting at a randomly chosen record key.
   * The number of records to scan is randomly chosen.
   * <p>
   * To model this within RedisSearch, we use FT.SEARCH and use computed hash score from the key name as the lower limit
   * for the search query, and set +inf as the upper limit of the search result.
   * The provided record count is passed via the LIMIT 0 <recordcound> FT.SEARCH argument.
   * <p>
   * Together, the above FT.SEARCH command arguments fully comply with a sorted, randomly chosen starting key, with
   * variadic record count replies.
   * <p>
   * Example FT.SEARCH command that a scan operation would generate.
   * "FT.SEARCH" "index" "*" \
   * "FILTER" __score__ "-6.17979116E8" +inf \
   * "LIMIT" "0" "54" \
   * "RETURN" "10" \
   * "field0" "field1" "field2" "field3" "field4" \
   * "field5" "field6" "field7" "field8" "field9"
   *
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
    try (Jedis j = getResource(startkey)) {
      int rangeStart = hash(startkey);
      int rangeEnd = Integer.MAX_VALUE;
      List<Object> resp = (List<Object>) j.sendCommand(RediSearchCommands.AGGREGATE,
          scanCommandArgs(indexName, recordcount, rangeStart, rangeEnd, fields));
      if (resultProcessing) {
        long totalResult = (long) resp.get(0);
        for (int i = 1; i < resp.size(); i++) {
          List<byte[]> docFields = (List<byte[]>) resp.get(i);
          HashMap<String, ByteIterator> values = new HashMap<>();
          for (int k = 2; k < docFields.size(); k += 2) {
            values.put(SafeEncoder.encode(docFields.get(k)),
                new StringByteIterator(SafeEncoder.encode(docFields.get(k + 1))));
            result.add(values);
          }
        }
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Helpher method to create the FT.SEARCH args used for the scan() operation.
   *
   * @param iName      RediSearch index name
   * @param rCount     return count
   * @param rangeStart numeric range start
   * @param rangeEnd   numeric range end
   * @param rFields    fields to retrieve
   * @return
   */
  private String[] scanCommandArgs(String iName, int rCount, int rangeStart, int rangeEnd, Set<String> rFields) {
    int returnFieldsCount = fieldCount;
    if (rFields != null) {
      returnFieldsCount = rFields.size();
    }
    ArrayList<String> scanSearchArgs = new ArrayList<>(Arrays.asList(iName,
        String.format("@%s:[%d +inf]", rangeField, rangeStart),
        "LIMIT", "0", String.valueOf(rCount - 1), "FIRST"));
    scanSearchArgs.addAll(Arrays.asList("SORTBY", "2", String.format("@%s", rangeField), "DESC"));
    scanSearchArgs.addAll(Arrays.asList("LOAD", String.valueOf(returnFieldsCount)));

    if (rFields == null) {
      for (int i = 0; i < returnFieldsCount; i++) {
        scanSearchArgs.add(String.format("%s%d", fieldPrefix, i));
      }
    } else {
      for (String field : rFields) {
        scanSearchArgs.add(field);
      }
    }
    return scanSearchArgs.toArray(String[]::new);
  }

  /**
   * RediSearch Protocol commands.
   */
  public enum RediSearchCommands implements ProtocolCommand {

    CREATE,
    AGGREGATE,
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
}
