package site.ycsb.db;

import io.lettuce.core.support.ConnectionPoolSupport;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.DirContextDnsResolver;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;

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
  public static final String READ_FROM = "redis.readfrom"; // replica_preferred, replica, master_preferred, master
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String CONNECTION_TIMEOUT_PROPERTY = "redis.con.timeout";
  public static final String COMMAND_TIMEOUT_PROPERTY = "redis.cmd.timeout";
  public static final String SSL_PROPERTY = "redis.ssl";

  public static final String CONNECTION_PROPERTY = "redis.con"; // connection mode
  public static final String CONNECTION_SINGLE = "single";  // single connection for all threads
  public static final String CONNECTION_MULTIPLE = "multi"; // multiple connections for all threads
  public static final String MULTI_SIZE_PROPERTY = "multi.size";  // connections amount for multi connection mode
  public static final String CONNECTION_POOLING = "pool"; // connection pool for all threads
  public static final String POOL_SIZE_PROPERTY = "pool.size"; // connection pool size for pooling mode

  // default Redis Settings
  private static final long DEFAULT_TIMEOUT_MILLIS = 2000L; // 2 seconds
//  private static final long DEFAULT_CONNECT_TIMEOUT_SECONDS = 2; // 2 seconds
  private static final int DEFAULT_REQUEST_QUEUE_SIZE = 65536; // 2^16 default is 2^31-1
  private static final int DEFAULT_RECONNECT_DELAY_SECONDS = 1;
  private static final int DEFAULT_TOPOLOGY_REFRESH_PERIOD_MINUTES = 60;  // 1 hour
//  private static final long DEFAULT_COMMAND_TIMEOUT_MILLIS = 2000L; // 2 seconds

  enum ConnectionMode {
    SINGLE, MULTIPLE, POOLING
  }

  public static final String INDEX_KEY = "_indices";

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static AbstractRedisClient client = null;
  private static ConnectionProvider stringConnectionProvider = null;
  private static boolean isCluster = false;

  /**
   * for redis cluster instances.
   * @param host
   * @param port
   * @param enableSSL
   * @return
   */
  static RedisClusterClient createClusterClient(String host, int port, boolean enableSSL,
      long connectTimeoutMillis, long commandTimeoutMillis) {
    DefaultClientResources resources = DefaultClientResources.builder()
        .dnsResolver(new DirContextDnsResolver())
        .reconnectDelay(Delay.constant(DEFAULT_RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS))
        .build();
    RedisURI primaryNode = RedisURI.builder()
        .withSsl(enableSSL)
        .withHost(host)
        .withPort(port)
        .build();

    RedisClusterClient clusterClient = RedisClusterClient.create(resources, primaryNode);

    ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
        .enableAllAdaptiveRefreshTriggers()
        .enablePeriodicRefresh(Duration.ofMinutes(DEFAULT_TOPOLOGY_REFRESH_PERIOD_MINUTES))
        .build();

    ClusterClientOptions clientOptions = ClusterClientOptions.builder()
        .requestQueueSize(DEFAULT_REQUEST_QUEUE_SIZE)
        .socketOptions(
            SocketOptions.builder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .build()
        )
        .timeoutOptions(
            TimeoutOptions.builder()
                .timeoutCommands(true)
                .fixedTimeout(Duration.ofMillis(commandTimeoutMillis))
                .build()
        )
        .topologyRefreshOptions(topologyRefreshOptions)
        .build();
    clusterClient.setOptions(clientOptions);

    return clusterClient;
  }

  static StatefulRedisClusterConnection<String, String> createConnection(
      RedisClusterClient clusterClient, ReadFrom readFrom) {
    StatefulRedisClusterConnection<String, String> clusterConnection = clusterClient.connect(StringCodec.UTF8);
    if (readFrom != null) {
      clusterConnection.setReadFrom(readFrom);
    }
    return clusterConnection;
  }

  /**
   * for redis instances not in a cluster.
   * @param host
   * @param port
   * @param enableSSL
   */
  static RedisClient createClient(long connectTimeoutMillis, long commandTimeoutMillis) {
    DefaultClientResources resources = DefaultClientResources.builder()
        .dnsResolver(new DirContextDnsResolver())
        .build();
    RedisClient redisClient = RedisClient.create(resources);

    ClientOptions clientOptions = ClientOptions.builder()
        .requestQueueSize(DEFAULT_REQUEST_QUEUE_SIZE)
        .socketOptions(
            SocketOptions.builder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .build()
        )
        .timeoutOptions(
            TimeoutOptions.builder()
                .timeoutCommands(true)
                .fixedTimeout(Duration.ofMillis(commandTimeoutMillis))
                .build()
        )
        .build();
    redisClient.setOptions(clientOptions);

    return redisClient;
  }

  static StatefulRedisMasterReplicaConnection<String, String> createConnection(RedisClient redisClient,
      List<String> hosts, int port, boolean enableSSL, ReadFrom readFrom) {
    List<RedisURI> nodes = getNodes(hosts, port, enableSSL);
    return createConnection(redisClient, nodes, readFrom);
  }

  static List<RedisURI> getNodes(List<String> hosts, int port, boolean enableSSL) {
    List<RedisURI> nodes = new ArrayList<RedisURI>();
    for (String host : hosts) {
      RedisURI node = RedisURI.builder()
          .withSsl(enableSSL)
          .withHost(host)
          .withPort(port)
          .build();
      nodes.add(node);
    }
    return nodes;
  }

  static StatefulRedisMasterReplicaConnection<String, String> createConnection(RedisClient redisClient,
      List<RedisURI> nodes, ReadFrom readFrom) {
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

    String timeoutString = props.getProperty(TIMEOUT_PROPERTY, Long.toString(DEFAULT_TIMEOUT_MILLIS));
    long timeout = Long.parseLong(timeoutString);
    long conTimeout = Long.parseLong(props.getProperty(CONNECTION_TIMEOUT_PROPERTY, Long.toString(timeout)));
    long cmdTimeout = Long.parseLong(props.getProperty(COMMAND_TIMEOUT_PROPERTY, Long.toString(timeout)));

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
        throw new DBException("unknown readfrom: " + readFromString);
      }
    }

    ConnectionMode connectionMode;
    String connectionModeString = props.getProperty(CONNECTION_PROPERTY, CONNECTION_SINGLE);
    if (CONNECTION_SINGLE.equals(connectionModeString)) {
      connectionMode = ConnectionMode.SINGLE;
    } else if (CONNECTION_MULTIPLE.equals(connectionModeString)) {
      connectionMode = ConnectionMode.MULTIPLE;
    } else if (CONNECTION_POOLING.equals(connectionModeString)) {
      connectionMode = ConnectionMode.POOLING;
    } else {
      throw new DBException("unknown connectionMode: " + connectionModeString);
    }

    if (clusterEnabled) {
      RedisClusterClient clusterClient = createClusterClient(host, port, sslEnabled, conTimeout, cmdTimeout);
      ConnectionProvider connectionProvider = null;
      if (connectionMode == ConnectionMode.SINGLE) {
        connectionProvider = new SingleConnectionProvider(clusterClient, readFrom);
      } else if (connectionMode == ConnectionMode.MULTIPLE) {
        int processors = Runtime.getRuntime().availableProcessors();
        int amount = Integer.parseInt(props.getProperty(MULTI_SIZE_PROPERTY, Integer.toString(processors)));
        connectionProvider = new MultipleConnectionsProvider(clusterClient, readFrom, amount);
      } else if (connectionMode == ConnectionMode.POOLING) {
        int processors = Runtime.getRuntime().availableProcessors();
        int poolSize = Integer.parseInt(props.getProperty(POOL_SIZE_PROPERTY, Integer.toString(processors)));
        connectionProvider = new PoolingConnectionsProvider(clusterClient, readFrom, poolSize);
      }

      client = clusterClient;
      stringConnectionProvider = connectionProvider;
      isCluster = true;
    } else {
      List<String> hosts = Arrays.asList(host.split(","));
      RedisClient redisClient = createClient(conTimeout, cmdTimeout);
      List<RedisURI> nodes = getNodes(hosts, port, sslEnabled);

      ConnectionProvider connectionProvider = null;
      if (connectionMode == ConnectionMode.SINGLE) {
        connectionProvider = new SingleConnectionProvider(redisClient, nodes, readFrom);
      } else if (connectionMode == ConnectionMode.MULTIPLE) {
        int processors = Runtime.getRuntime().availableProcessors();
        int amount = Integer.parseInt(props.getProperty(MULTI_SIZE_PROPERTY, Integer.toString(processors)));
        connectionProvider = new MultipleConnectionsProvider(redisClient, nodes, readFrom, amount);
      } else if (connectionMode == ConnectionMode.POOLING) {
        int processors = Runtime.getRuntime().availableProcessors();
        int poolSize = Integer.parseInt(props.getProperty(POOL_SIZE_PROPERTY, Integer.toString(processors)));
        connectionProvider = new PoolingConnectionsProvider(redisClient, nodes, readFrom, poolSize);
      }

      client = redisClient;
      stringConnectionProvider = connectionProvider;
      isCluster = false;
    }
  }

  private void shutdownConnection() throws DBException {
    if (stringConnectionProvider != null) {
      try {
        stringConnectionProvider.close();
      } catch(Exception ex) {
        // ignore
      }
      stringConnectionProvider = null;
    }

    if (client != null) {
      client.close();
      client = null;
    }
  }

  private RedisClusterCommands<String, String> getRedisClusterCommands(
      StatefulConnection<String, String> stringConnection) {
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

  private Status withRedisClusterCommands(Function<RedisClusterCommands, Status> function) {
    try {
      StatefulConnection<String, String> stringConnection = stringConnectionProvider.getConnection();
      try {
        RedisClusterCommands<String, String> cmds = getRedisClusterCommands(stringConnection);
        return function.apply(cmds);
      } catch (RedisCommandTimeoutException e) {
        return Status.TIMEOUT;
      } finally {
        stringConnectionProvider.returnConnection(stringConnection);
      }
    } catch (Exception e) { // getConnection exception
      return Status.ERROR;
    }
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
    return withRedisClusterCommands((cmds) -> {
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
    );
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
    return withRedisClusterCommands((cmds) -> {
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
    );
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
    return withRedisClusterCommands((cmds) -> {
        return cmds.hmset(key, StringByteIterator.getStringMap(values))
            .equals("OK") ? Status.OK : Status.ERROR;
      }
    );
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
    return withRedisClusterCommands((cmds) -> {
        if (cmds.hmset(key, StringByteIterator.getStringMap(values))
            .equals("OK")) {
          cmds.zadd(INDEX_KEY, hash(key), key);
          return Status.OK;
        }
        return Status.ERROR;
      }
    );
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
    return withRedisClusterCommands((cmds) -> {
        return cmds.del(key) == 0 && cmds.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
              : Status.OK;
      }
    );
  }

  private interface ConnectionProvider extends AutoCloseable {
    StatefulConnection<String, String> getConnection() throws Exception;
    void returnConnection(StatefulConnection<String, String> connection);
  }

  private static class SingleConnectionProvider implements ConnectionProvider {

    private StatefulConnection<String, String> stringConnection = null;

    public SingleConnectionProvider(RedisClusterClient clusterClient, ReadFrom readFrom) {
      this.stringConnection = createConnection(clusterClient, readFrom);
    }

    public SingleConnectionProvider(RedisClient redisClient, List<RedisURI> nodes, ReadFrom readFrom) {
      this.stringConnection = createConnection(redisClient, nodes, readFrom);
    }

    @Override
    public StatefulConnection<String, String> getConnection() throws Exception {
      return stringConnection;
    }

    @Override
    public void returnConnection(StatefulConnection<String, String> connection) {
      // do nothing
    }

    @Override
    public void close() throws Exception {
      if (stringConnection != null) {
        stringConnection.close();
        stringConnection = null;
      }
    }
  }

  private static class MultipleConnectionsProvider implements ConnectionProvider {
    private List<StatefulConnection<String, String>> stringConnections =
        new ArrayList<StatefulConnection<String, String>>();

    public MultipleConnectionsProvider(RedisClusterClient clusterClient, ReadFrom readFrom, int amount) {
      for (int i = 0; i < amount; i++) {
        this.stringConnections.add(createConnection(clusterClient, readFrom));
      }
    }

    public MultipleConnectionsProvider(RedisClient redisClient, List<RedisURI> nodes, ReadFrom readFrom, int amount) {
      for (int i = 0; i < amount; i++) {
        this.stringConnections.add(createConnection(redisClient, nodes, readFrom));
      }
    }

    @Override
    public StatefulConnection<String, String> getConnection() throws Exception {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      return this.stringConnections.get(random.nextInt(0, this.stringConnections.size()));
    }

    @Override
    public void returnConnection(StatefulConnection<String, String> connection) {
      // do nothing
    }

    @Override
    public void close() throws Exception {
      for (StatefulConnection<String, String> stringConnection: stringConnections) {
        stringConnection.close();
      }
      stringConnections.clear();
    }
  }

  private static class PoolingConnectionsProvider implements ConnectionProvider {

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> clusterConnectionPool = null;
    private GenericObjectPool<StatefulRedisMasterReplicaConnection<String, String>> masterReplicaConnectionPool = null;
    private boolean isCluster = false;


    public PoolingConnectionsProvider(RedisClusterClient clusterClient, ReadFrom readFrom, int poolSize) {
      GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig =
          new GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>>();
      poolConfig.setMaxTotal(poolSize);
      poolConfig.setMaxIdle(poolSize);
      clusterConnectionPool = ConnectionPoolSupport.createGenericObjectPool(
          () -> { return createConnection(clusterClient, readFrom); }, poolConfig);
      isCluster = true;
    }

    public PoolingConnectionsProvider(RedisClient redisClient, List<RedisURI> nodes, ReadFrom readFrom, int poolSize) {
      GenericObjectPoolConfig<StatefulRedisMasterReplicaConnection<String, String>> poolConfig =
          new GenericObjectPoolConfig<StatefulRedisMasterReplicaConnection<String, String>>();
      poolConfig.setMaxTotal(poolSize);
      poolConfig.setMaxIdle(poolSize);
      masterReplicaConnectionPool = ConnectionPoolSupport.createGenericObjectPool(
          () -> { return createConnection(redisClient, nodes, readFrom); }, poolConfig);
    }

    @Override
    public StatefulConnection<String, String> getConnection() throws Exception {
      return isCluster ? clusterConnectionPool.borrowObject() : masterReplicaConnectionPool.borrowObject();
    }

    @Override
    public void returnConnection(StatefulConnection<String, String> connection) {
      if (isCluster) {
        clusterConnectionPool.returnObject((StatefulRedisClusterConnection<String, String>)connection);
      } else {
        masterReplicaConnectionPool.returnObject((StatefulRedisMasterReplicaConnection<String, String>)connection);
      }
    }

    @Override
    public void close() throws Exception {
      if (clusterConnectionPool != null) {
        clusterConnectionPool.close();
        clusterConnectionPool = null;
      }
      if (masterReplicaConnectionPool != null) {
        masterReplicaConnectionPool.close();
        masterReplicaConnectionPool = null;
      }
    }
  }

}
