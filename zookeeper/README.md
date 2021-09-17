<!--
Copyright (c) 2020 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on ZooKeeper.

### 1. Start ZooKeeper Server(s)

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    # more details in the landing page for instructions on downloading YCSB(https://github.com/brianfrankcooper/YCSB#getting-started).
    cd YCSB
    mvn -pl site.ycsb:zookeeper-binding -am clean package -DskipTests

### 4. Provide ZooKeeper Connection Parameters

Set connectString, sessionTimeout, watchFlag in the workload you plan to run.

- `zookeeper.connectString`
- `zookeeper.sessionTimeout`
- `zookeeper.watchFlag`
  * A parameter for enabling ZooKeeper's watch, optional values:true or false.the default value is false.
  * This parameter cannot test the watch performance, but for testing what effect will take on the read/write requests when enabling the watch.

      ```bash
      ./bin/ycsb run zookeeper -s -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p zookeeper.watchFlag=true
      ```

Or, you can set configs with the shell command, EG:

    # create a /benchmark namespace for sake of cleaning up the workspace after test.
    # e.g the CLI:create /benchmark
    ./bin/ycsb run zookeeper -s -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p zookeeper.sessionTimeout=30000

### 5. Load data and run tests

Load the data:

    # -p recordcount,the count of records/paths you want to insert
    ./bin/ycsb load zookeeper -s -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p recordcount=10000 > outputLoad.txt

Run the workload test:

    # YCSB workloadb is the most suitable workload for read-heavy workload for the ZooKeeper in the real world.

    # -p fieldlength, test the length of value/data-content took effect on performance
    ./bin/ycsb run zookeeper -s -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p fieldlength=1000

    # -p fieldcount
    ./bin/ycsb run zookeeper -s -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p fieldcount=20

    # -p hdrhistogram.percentiles,show the hdrhistogram benchmark result
    ./bin/ycsb run zookeeper -threads 1 -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p hdrhistogram.percentiles=10,25,50,75,90,95,99,99.9 -p histogram.buckets=500

    # -threads: multi-clients test, increase the **maxClientCnxns** in the zoo.cfg to handle more connections.
    ./bin/ycsb run zookeeper -threads 10 -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark

    # show the timeseries benchmark result
    ./bin/ycsb run zookeeper -threads 1 -P workloads/workloadb -p zookeeper.connectString=127.0.0.1:2181/benchmark -p measurementtype=timeseries -p timeseries.granularity=50

    # cluster test
    ./bin/ycsb run zookeeper -P workloads/workloadb -p zookeeper.connectString=192.168.10.43:2181,192.168.10.45:2181,192.168.10.27:2181/benchmark

    # test leader's read/write performance by setting zookeeper.connectString to leader's(192.168.10.43:2181)
    ./bin/ycsb run zookeeper -P workloads/workloadb -p zookeeper.connectString=192.168.10.43:2181/benchmark

    # test for large znode(by default: jute.maxbuffer is 1048575 bytes/1 MB ). Notice:jute.maxbuffer should also be set the same value in all the zk servers.
    ./bin/ycsb run zookeeper -jvm-args="-Djute.maxbuffer=4194304" -s -P workloads/workloadc -p zookeeper.connectString=127.0.0.1:2181/benchmark

    # Cleaning up the workspace after finishing the benchmark.
    # e.g the CLI:deleteall /benchmark