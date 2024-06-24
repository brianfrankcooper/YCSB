<!--
Copyright (c) 2023 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Redis.

### 1. Start etcd

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:etcd-binding -am clean package -DskipTests

### 4. Provide etcd Connection Parameters and start

First, start etcd server. Then set etcd cluster's endpoints in the workload you plan to run.

- `etcd.endpoints`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load etcd -s -P workloads/workloada -p "etcd.endpoints=http://127.0.0.1:2379" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load etcd -s -P workloads/workloada -p "etcd.endpoints=http://127.0.0.1:2379" > outputLoad.txt

Run the workload test:

    ./bin/ycsb run etcd -s -P workloads/workloada -p "etcd.endpoints=http://127.0.0.1:2379" > outputRun.txt

