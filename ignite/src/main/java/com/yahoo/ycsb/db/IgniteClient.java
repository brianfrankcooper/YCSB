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
public class IgniteClient {

  private static final Logger LOG = LoggerFactory.getLogger(IgniteClient.class);

  static Ignite startIgnite() {

    String ipAddresses = System.getProperty("igniteIps", "127.0.0.1,127.0.0.1:47500..47509");
    LOG.info("starting ignite with seed: {} \n\tto change them set property igniteIps to comma separated list of IPs",ipAddresses);

    TcpDiscoverySpi spi = new TcpDiscoverySpi();

    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

    // Set initial IP addresses.
    // Note that you can optionally specify a port or a port range.
    ipFinder.setAddresses(Arrays.asList(ipAddresses.split(",")));

    spi.setIpFinder(ipFinder);

    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setClientMode(true);
    // Override default discovery SPI.
    cfg.setDiscoverySpi(spi);
    return Ignition.start(cfg);
  }
}
