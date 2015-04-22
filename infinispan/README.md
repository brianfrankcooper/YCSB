## Quick Start

This section describes how to run YCSB on infinispan. 

### 1. Install Java and Maven

### 2. Set Up YCSB
1. Git clone YCSB and compile:
  ```
git clone http://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
  ```

2. Copy and untar YCSB distribution in distribution/target/ycsb-x.x.x.tar.gz to target machine

### 4. Load data and run tests
####4.1 embedded mode with cluster or not
Load the data:
```
./bin/ycsb load infinispan -P workloads/workloada -p infinispan.clustered=<true or false>
```
Run the workload test:
```
./bin/ycsb run infinispan -s -P workloads/workloada -p infinispan.clustered=<true or false>
```
####4.2 client-server mode
    
1. start infinispan server

2. read [RemoteCacheManager](http://docs.jboss.org/infinispan/7.2/apidocs/org/infinispan/client/hotrod/RemoteCacheManager.html) doc and customize hotrod client properties in infinispan-binding/conf/remote-cache.properties

3. Load the data with specified cache:
  ```
./bin/ycsb load infinispan-cs -s -P workloads/workloada -P infinispan-binding/conf/remote-cache.properties -p cache=<cache name>
  ```

4. Run the workload test with specified cache:
  ```
./bin/ycsb run infinispan-cs -s -P workloads/workloada -P infinispan-binding/conf/remote-cache.properties -p cache=<cache name>
  ```