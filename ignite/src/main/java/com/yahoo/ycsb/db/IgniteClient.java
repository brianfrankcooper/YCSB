package com.yahoo.ycsb.db;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 *
 */
public final class  IgniteClient {

  private IgniteClient() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(IgniteClient.class);

  static Ignite startIgnite(String igniteIPs, boolean clientMode) {


    LOG.info("starting ignite with seed: {} \n\tset property " +
        IgniteYCSBClient.IGNITE_IPS+" to comma separated list of IPs", igniteIPs);

    TcpDiscoverySpi spi = new TcpDiscoverySpi();

    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

    // Set initial IP addresses.
    // Note that you can optionally specify a port or a port range.
    ipFinder.setAddresses(Arrays.asList(igniteIPs.split(",")));

    spi.setIpFinder(ipFinder);

    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setClientMode(clientMode);
    cfg.setIgniteInstanceName((clientMode)? "client":"gridNode-"+System.currentTimeMillis());
    // Override default discovery SPI.
    cfg.setDiscoverySpi(spi);
    return Ignition.start(cfg);
  }
}
