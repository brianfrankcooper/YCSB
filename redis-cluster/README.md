<!--
Copyright (c) 2016 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Redis Cluster.

### 1. Start Redis Cluster

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:redis-cluster-binding -am clean package

### 4. Provide Redis Cluster Connection Parameters

- `redis.cluster.host`
- `redis.cluster.port`
- `redis.cluster.password`
- `redis.cluster.pool.maxclients`

Or, you can set configs with the command, e.g.:

    ./bin/ycsb load redis -s -P workloads/workloada -p "redis.cluster.host=127.0.0.1" -p "redis.cluster.port=7001" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load redis-cluster -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redis-cluster -s -P workloads/workloada > outputRun.txt
