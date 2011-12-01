This directory should contain gemfire.jar OR gemfire.jar can be added to CLASSPATH for compiling GemFireClient.

GemFireClient can be compiled using target:
$ ant dbcompile-gemfire

Running benchmark.
1. Copy cache.xml from this dir to your GemFire install directory ($GEMFIRE_HOME)
2. start GemFire cache server
  - $ cd $GEMFIRE_HOME
  - $ bin/cacheserver start -J-Xms42g -J-Xmx42g -J-XX:+UseConcMarkSweepGC -J-XX:CMSInitiatingOccupancyFraction=70
3. Add ycsb.jar and gemfire.jar to CLASSPATH.
4. run YCSB workload.

GemFire can be run either in client-server or peer-to-peer mode.
By default com.yahoo.ycsb.db.GemFireClient connects as a client to GemFire server running on localhost on default port (40404). host name and port of a GemFire server running elsewhere can be specified by properties "gemfire.serverhost" and "gemfire.serverport" respectively. Example:
$ java com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.GemFireClient -P workloads/workloada -p gemfire.serverhost=host2 -p gemfire.serverport=3333

To run com.yahoo.ycsb.db.GemFireClient as a peer to existing GemFire members, use property "gemfire.topology" like so:
$ java com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.GemFireClient -P workloads/workloada -p gemfire.topology=p2p 

Locators can be used for member discovery, either in client-server or peer-to-peer mode. Please see GemFire docs for details. locators can be specified like so:
$ java com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.GemFireClient -P workloads/workloada -p gemfire.locator=host1[port1],host2[port2]

Please refer to GemFire docs here: https://www.vmware.com/support/pubs/vfabric-gemfire.html.
Questions? visit: http://forums.gemstone.com
