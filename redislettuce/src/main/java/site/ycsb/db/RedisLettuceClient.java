package site.ycsb.db;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DirContextDnsResolver;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for Redis using Lettuce client.
 */
public class RedisLettuceClient extends DB {

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final int DEFAULT_PORT = 6379;
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String READ_FROM = "redis.readfrom";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String SSL_PROPERTY = "redis.ssl";

  public static final String INDEX_KEY = "_indices";

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static AbstractRedisClient client = null;
  private static StatefulConnection<String, String> stringConnection = null;
  private static boolean isCluster = false;

  /**
   * for redis cluster instances.
   * @param host
   * @param port
   * @param enableSSL
   * @return
   */
  private RedisClusterClient createClusterClient(String host, int port, boolean enableSSL) {
    DefaultClientResources resources = DefaultClientResources.builder()
        .dnsResolver(new DirContextDnsResolver())
        .build();
    RedisURI primaryNode = RedisURI.builder()
        .withSsl(enableSSL)
        .withHost(host)
        .withPort(port)
        .build();

    RedisClusterClient clusterClient = RedisClusterClient.create(resources, primaryNode);
    return clusterClient;
  }

  /**
   * for redis instances not in a cluster.
   * @param host
   * @param port
   * @param enableSSL
   * @return
   */
  private RedisClient createClient() {
    DefaultClientResources resources = DefaultClientResources.builder()
        .dnsResolver(new DirContextDnsResolver())
        .build();
    RedisClient redisClient = RedisClient.create(resources);
    return redisClient;
  }

  private StatefulRedisMasterReplicaConnection<String, String> createConnection(RedisClient redisClient,
      List<String> hosts, int port, boolean enableSSL, ReadFrom readFrom) {
    List<RedisURI> nodes = new ArrayList<RedisURI>();
    for (String host : hosts) {
      RedisURI node = RedisURI.builder()
          .withSsl(enableSSL)
          .withHost(host)
          .withPort(port)
          .build();
      nodes.add(node);
    }
    StatefulRedisMasterReplicaConnection<String, String> masterReplicaConnection =
        MasterReplica.connect(redisClient, StringCodec.UTF8, nodes);
    if (readFrom != null) {
      masterReplicaConnection.setReadFrom(readFrom);
    }
    return masterReplicaConnection;
  }

  private void setupConnection() throws DBException {
    if (client != null) {
      return;
    }

    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    boolean sslEnabled = Boolean.parseBoolean(props.getProperty(SSL_PROPERTY, "false"));

    ReadFrom readFrom = null;
    String readFromString = props.getProperty(READ_FROM);
    if (readFromString != null) {
      if ("replica_preferred".equals(readFromString)) {
        readFrom = ReadFrom.REPLICA_PREFERRED;
      } else if ("master_preferred".equals(readFromString)) {
        readFrom = ReadFrom.MASTER_PREFERRED;
      } else if ("master".equals(readFromString)) {
        readFrom = ReadFrom.MASTER;
      } else if ("replica".equals(readFromString)) {
        readFrom = ReadFrom.REPLICA;
      } else {
        throw new DBException("readfrom " + readFromString + " not support");
      }
    }

    if (clusterEnabled) {
      RedisClusterClient clusterClient = createClusterClient(host, port, sslEnabled);
      StatefulRedisClusterConnection<String, String> clusterConnection = clusterClient.connect(StringCodec.UTF8);
      if (readFrom != null) {
        clusterConnection.setReadFrom(readFrom);
      }
      client = clusterClient;
      stringConnection = clusterConnection;
      isCluster = true;
    } else {
      List<String> hosts = Arrays.asList(host.split(","));
      RedisClient redisClient = createClient();
      StatefulRedisMasterReplicaConnection masterReplicaConnection = createConnection(redisClient, hosts,
          port, sslEnabled, readFrom);
      client = redisClient;
      stringConnection = masterReplicaConnection;
      isCluster = false;
    }
  }

  private void shutdownConnection() throws DBException {
    stringConnection.close();
    stringConnection = null;

    client.close();
    client = null;
  }

  private RedisClusterCommands<String, String> getRedisClusterCommands() {
    RedisClusterCommands cmds = null;
    if (stringConnection != null) {
      if (isCluster) {
        cmds = ((StatefulRedisClusterConnection<String, String>)stringConnection).sync();
      } else {
        cmds = ((StatefulRedisMasterReplicaConnection<String, String>)stringConnection).sync();
      }
    }
    return cmds;
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // connection instance for all the threads.
    synchronized (INIT_COUNT) {
      setupConnection();
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        shutdownConnection();
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    RedisClusterCommands<String, String> cmds = getRedisClusterCommands();
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, cmds.hgetall(key));
    } else {
      String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
      List<KeyValue<String, String>> values = cmds.hmget(key, fieldArray);

      Iterator<KeyValue<String, String>> fieldValueIterator = values.iterator();

      while (fieldValueIterator.hasNext()) {
        KeyValue<String, String> fieldValue = fieldValueIterator.next();
        result.put(fieldValue.getKey(),
            new StringByteIterator(fieldValue.getValue()));
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  private double hash(String key) {
    return key.hashCode();
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    RedisClusterCommands<String, String> cmds = getRedisClusterCommands();
    List<String> keys = cmds.zrangebyscore(INDEX_KEY,
        Range.from(Range.Boundary.excluding(hash(startkey)), Range.Boundary.excluding(Double.POSITIVE_INFINITY)),
        Limit.from(recordcount));

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    RedisClusterCommands<String, String> cmds = getRedisClusterCommands();
    return cmds.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    RedisClusterCommands<String, String> cmds = getRedisClusterCommands();
    if (cmds.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      cmds.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  @Override
  public Status delete(String table, String key) {
    RedisClusterCommands<String, String> cmds = getRedisClusterCommands();
    return cmds.del(key) == 0 && cmds.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

}
