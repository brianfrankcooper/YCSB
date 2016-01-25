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

This section describes how to run YCSB on [Accumulo](https://accumulo.apache.org/). 

### 1. Start Accumulo

See the [Accumulo Documentation](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_installation)
for details on installing and running Accumulo.

Before running the YCSB test you must create the Accumulo table. Again see the 
[Accumulo Documentation](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_basic_administration)
for details. The default table name is `ycsb`.

### 2. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:aerospike-binding -am clean package

### 3. Load Data and Run Tests

Load the data:

    ./bin/ycsb load accumulo -s -P workloads/workloada \
         -p accumulo.zooKeepers=localhost \
         -p accumulo.columnFamily=ycsb \
         -p accumulo.instanceName=ycsb \
         -p accumulo.username=user \
         -p accumulo.password=supersecret \
         > outputLoad.txt

Run the workload test:

    ./bin/ycsb run accumulo -s -P workloads/workloada  \
         -p accumulo.zooKeepers=localhost \
         -p accumulo.columnFamily=ycsb \
         -p accumulo.instanceName=ycsb \
         -p accumulo.username=user \
         -p accumulo.password=supersecret \
         > outputLoad.txt

## Accumulo Configuration Parameters

- `accumulo.zooKeepers`
  - The Accumulo cluster's [zookeeper servers](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_connecting).
  - Should contain a comma separated list of of hostname or hostname:port values.
  - No default value.

- `accumulo.columnFamily`
  - The name of the column family to use to store the data within the table.
  - No default value.

- `accumulo.instanceName`
  - Name of the Accumulo [instance](https://accumulo.apache.org/1.7/accumulo_user_manual.html#_connecting).
  - No default value.

- `accumulo.username`
  - The username to use when connecting to Accumulo.
  - No default value.
 
- `accumulo.password`
  - The password for the user connecting to Accumulo.
  - No default value.

