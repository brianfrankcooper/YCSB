package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ignite abstract client.
 * <p>
 * See {@code ignite/README.md} for details.
 *
 * @author Sergey Puchnin
 * @author Taras Ledkov
 * @author Oleg Ostanin
 */
public abstract class IgniteAbstractClient extends DB {
  /** */
  protected static Logger log = LogManager.getLogger(IgniteAbstractClient.class);
  
  protected static final String DEFAULT_CACHE_NAME = "usertable";
  protected static final String HOSTS_PROPERTY = "hosts";
  protected static final String PORTS_PROPERTY = "ports";
  protected static final String CLIENT_NODE_NAME = "YCSB client node";
  protected static final String PORTS_DEFAULTS = "47500..47509";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  protected static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  /** Ignite cluster. */
  protected static Ignite cluster = null;
  /** Ignite cache to store key-values. */
  protected static IgniteCache<String, BinaryObject> cache = null;
  /** Debug flag. */
  protected static boolean debug = false;

  protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);


  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }

      try {
        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        IgniteConfiguration igcfg = new IgniteConfiguration();
        igcfg.setIgniteInstanceName(CLIENT_NODE_NAME);

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              HOSTS_PROPERTY));
        }

        String ports = getProperties().getProperty(PORTS_PROPERTY, PORTS_DEFAULTS);

        if (ports == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              PORTS_PROPERTY));

        }

        System.setProperty("IGNITE_QUIET", "false");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        Collection<String> addrs = new LinkedHashSet<>();
        addrs.add(host + ":" + ports);

        ((TcpDiscoveryVmIpFinder) ipFinder).setAddresses(addrs);
        disco.setIpFinder(ipFinder);

        igcfg.setDiscoverySpi(disco);
        igcfg.setNetworkTimeout(2000);
        igcfg.setClientMode(true);

        Log4J2Logger logger = new Log4J2Logger(this.getClass().getClassLoader().getResource("log4j2.xml"));
        igcfg.setGridLogger(logger);

        log.info("Start Ignite client node.");
        cluster = Ignition.start(igcfg);

        log.info("Activate Ignite cluster.");
        cluster.active(true);

        cache = cluster.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        if(cache == null) {
          throw new DBException(new IgniteCheckedException("Failed to find cache " + DEFAULT_CACHE_NAME));
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        cluster.close();
        cluster = null;
      }

      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }
}
