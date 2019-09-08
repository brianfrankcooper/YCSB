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

This section describes how to run YCSB on Apache Crail. 

### 1. Start Crail

    https://incubator-crail.readthedocs.io

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:crail-binding -am clean package

### 4. Provide Crail Connection Parameters

Connction parameters have to be defined in $CRAIL_HOME/conf/crail-site.conf. 

  * `crail.namenode.address` - The Crail cluster to connect to (default: `crail://namenode:9060`)
  * `crail.blocksize` - The block size (bytes) of the Crail cluster (default: `1048576`)
  * `crail.buffersize` - The buffer size (bytes) used by the client (default: `crail.blocksize`)
  * `crail.cachelimit` - Maximum client side cache (bytes) (default: `1073741824`)
  * `crail.cachepath` - Directory where to mmap memory from (no default)
  * `crail.storage.types` - Comma separated list of storage tiers (default: `org.apache.crail.storage.tcp.TcpStorageTier`)

The following benchmark parameters are available.

  * `crail.enumeratekeys` - Whether to make keys visible for enumeration or not (default: `false`)

Add them to the workload or set them with the shell command, as in:

    ./bin/ycsb load crail -s -P workloads/workloada -p crail.enumeratekeys=true >outputLoad.txt


### 5. Load Data and Run Tests

Load the data:

    ./bin/ycsb load crail -s -P workloads/workloada 

Run the workload test:

    ./bin/ycsb run crail -s -P workloads/workloada 

