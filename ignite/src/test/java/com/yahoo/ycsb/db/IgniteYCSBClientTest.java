package com.yahoo.ycsb.db;

import com.yahoo.ycsb.workloads.CoreWorkload;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 */
public class IgniteYCSBClientTest {

  static Ignite ignite;
  static IgniteCache<Object, Object> cache;

  @BeforeClass
  public static void setup() {
//      ignite = startIgnite();
//      cache =  ignite.createCache( System.getProperty( CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT ));

  }


  @Test
  public void testLoadAsyncBatch() {
    com.yahoo.ycsb.Client.main(new String[]{
        "-load",
        "-db", "com.yahoo.ycsb.db.IgniteYCSBClient",
        "-p", "workload=com.yahoo.ycsb.workloads.CoreWorkload",
        "-p", "insertAsync=true",
        "-p", "batchSize=10",
        "-p", "threadcount=2",
        "-p", "recordcount=100",
        "-p", "operationcount=100",
    });
  }


  @Test
    public void testLoadSynk() {
      com.yahoo.ycsb.Client.main(new String[]{
          "-load",
          "-db", "com.yahoo.ycsb.db.IgniteYCSBClient",
          "-p", "workload=com.yahoo.ycsb.workloads.CoreWorkload",
          "-p", "insertAsync=false",
          "-p", "batchSize=1",
          "-p", "threadcount=2",
          "-p", "recordcount=100",
          "-p", "operationcount=100",
      });
    }


  @Test
   public void testRun() {
     com.yahoo.ycsb.Client.main(new String[]{
         "-t",
         "-db", "com.yahoo.ycsb.db.IgniteYCSBClient",
         "-p", "workload=com.yahoo.ycsb.workloads.CoreWorkload",
         "-p", "threadcount=1",
         "-p", "recordcount=100",
         "-p", "operationcount=100",
         "-p", "readproportion=0.5",
         "-p", "updateproportion=0.5",
     });
   }


}
