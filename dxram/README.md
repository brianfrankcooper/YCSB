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

Get the output *.tar.gz* from *dxram/target* and unpack it to your target machine. Run the following *./bin/ycsb* commands from the unpacked distribution package not the source folder.

### 3. Set Up DXRAM

Either build DXRAM or download the pre-built binaries and follow the instructions of our quick start guide in the README of the [DXRAM repository](https://github.com/hhu-bsinfo/dxram) to setup a DXRAM cluster.

### 4. Run the YCSB client

Use our deployment tool [cdepl](https://github.com/hhu-bsinfo/cdepl) for quick and easy deployment of DXRAM storage instances and YCSB client instances.

First, please refer to the README in the DXRAM repository for manual deployment and configuration of a DXRAM cluster and single instances of it.

Use the ycsb script from *bin/* to manually run YCSB clients.

Example running workloada with a DXRAM cluster consisting of one superpeer and one storage peer and minimal test configuration (low record and thread count).

Load client:
```
./bin/ycsb load dxram -P workloads/workloada -p insertorder=ordered -p fieldcount=10 -p fieldlength=100 -p dxram.bind=<IP ADDRESS OF YOUR INSTANCE>:<PORT OF YOUR INSTANCE (e.g. 22222)> -p dxram.join=<IP ADDRESS OF ZOOKEEPER INSTANCE>:<PORT OF ZOOKEEPER INSTANCE (e.g. 2181)> -threads 1
```

Make sure to adjust/replace values according to your setup (e.g. IP addresses, ports, storage counts etc.).

DXRAM YCSB client required parameters (omitting core YCSB parameters):
* *-p dxram.bind=\<IP ADDRESS OF YOUR INSTANCE\>:\<PORT OF YOUR INSTANCE\>*: IPv4 address and port to bind DXRAM the instance to.
* *-p dxram.join=\<IP ADDRESS OF ZOOKEEPER INSTANCE>:\<PORT OF ZOOKEEPER INSTANCE\>*: IPv4 address and port of ZooKeeper instance DXRAM is using for bootstrapping. ZooKeeper uses port 2181 by default.
* *-p insertorder=ordered*: Required because DXRAM does not support "hashed"

Optional parameters (only recommended for experienced DXRAM users):
* *-p dxram.network*: The network device, that DXRAM shall use. Can either be set to *ethernet* or *infiniband* (default: *ethernet*).
* *-p dxram.pooling*: Set this to *false* to disable object pooling (default: *true*).
* *-p dxram.distribution*: The strategy used to distribute records among the DXRAM storage nodes. Valid values are *linear* and *scattered* (default: *linear*)

Benchmark client:
```
./bin/ycsb run dxram -P workloads/workloada -p insertorder=ordered -p fieldcount=10 -p fieldlength=100 -p dxram.bind=<IP ADDRESS OF YOUR INSTANCE>:<PORT OF YOUR INSTANCE (e.g. 22222)> -p dxram.join=<IP ADDRESS OF ZOOKEEPER INSTANCE>:<PORT OF ZOOKEEPER INSTANCE (e.g. 2181)> -threads 1
```
