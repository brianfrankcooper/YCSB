/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */
package com.yahoo.ycsb.db.ignite;

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
