<!--
Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.

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

This section describes how to run the YCSB on [DXRAM](https://github.com/hhu-bsinfo/dxram).

### 1. Install Java and Maven

### 2. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:dxram-binding -am clean package

### 3. Set Up DXRAM

Either build DXRAM or download the pre-built binaries and follow the instructions of our quick start guide in the README of the [DXRAM repository](https://github.com/hhu-bsinfo/dxram) to setup a DXRAM cluster.

### 4. Run the YCSB client

Use our deployment tool [cdepl](https://github.com/hhu-bsinfo/cdepl) for quick and easy deployment of DXRAM storage instances and YCSB client instances.

First, please refer to the README in the DXRAM repository for manual deployment and configuration of a DXRAM cluster and single instances of it.

Use the ycsb script from *bin/* to manually run YCSB clients. You can and also must override some DXRAM settings using JVM arguments or use multiple configuration files (see also DXRAM setup guide).
Load client:
```
./bin/ycsb load dxram -jvm-args -Ddxram.config=<path to dxram configuration file> -Ddxram.m_engineConfig.m_address.m_ip=<IP address of the current instance> -Ddxram.m_engineConfig.m_address.m_port=<port to use for current instance> -Ddxram.m_componentConfigs[ZookeeperBootComponent].m_connection.m_ip=<IP of Zookeeper server> -Ddxram.m_componentConfigs[ZookeeperBootComponent].m_connection.m_port=<port of Zookeeper server> -Ddxram.m_engineConfig.m_role=Peer -P workloads/workloada -p insertorder=ordered -p dxram.stores=<total number of storage nodes in the target cluster> -p dxram.recordsPerStoreNode=<per node record count> -p dxram.load.targetNodeIdx=<load data to the X-th node of the total number of storage nodes, e.g. 0 for first, 1 for second etc>
```

Benchmark client:
```
./bin/ycsb load dxram -jvm-args -Ddxram.config=<path to dxram configuration file> -Ddxram.m_engineConfig.m_address.m_ip=<IP address of the current instance> -Ddxram.m_engineConfig.m_address.m_port=<port to use for current instance> -Ddxram.m_componentConfigs[ZookeeperBootComponent].m_connection.m_ip=<IP of Zookeeper server> -Ddxram.m_componentConfigs[ZookeeperBootComponent].m_connection.m_port=<port of Zookeeper server> -Ddxram.m_engineConfig.m_role=Peer -P workloads/workloada -p insertorder=ordered -p dxram.stores=<total number of storage nodes in the target cluster>
```

Note: DXRAM requires insertorder=ordered and does not support "hashed".
