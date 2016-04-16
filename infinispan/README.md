<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

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
-->

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