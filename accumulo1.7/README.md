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
    mvn -pl com.yahoo.ycsb:accumulo1.7-binding -am clean package

### 3. Create the Accumulo table

By default, YCSB uses a table with the name "usertable". Users must create this table before loading
data into Accumulo. For maximum Accumulo performance, the Accumulo table must be pre-split. A simple
Ruby script, based on the HBase README, can generate adequate split-point. 10's of Tablets per
TabletServer is a good starting point. Unless otherwise specified, the following commands should run
on any version of Accumulo.

    $ echo 'num_splits = 20; puts (1..num_splits).map {|i| "user#{1000+i*(9999-1000)/num_splits}"}' | ruby > /tmp/splits.txt
    $ accumulo shell -u <user> -p <password> -e "createtable usertable"
    $ accumulo shell -u <user> -p <password> -e "addsplits -t usertable -sf /tmp/splits.txt"
    $ accumulo shell -u <user> -p <password> -e "config -t usertable -s table.cache.block.enable=true"

Additionally, there are some other configuration properties which can increase performance. These
can be set on the Accumulo table via the shell after it is created. Setting the table durability
to `flush` relaxes the constraints on data durability during hard power-outages (avoids calls
to fsync). Accumulo defaults table compression to `gzip` which is not particularly fast; `snappy`
is a faster and similarly-efficient option. The mutation queue property controls how many writes
that Accumulo will buffer in memory before performing a flush; this property should be set relative
to the amount of JVM heap the TabletServers are given.

Please note that the `table.durability` and `tserver.total.mutation.queue.max` properties only
exists for >=Accumulo-1.7. There are no concise replacements for these properties in earlier versions.

    accumulo> config -s table.durability=flush
    accumulo> config -s tserver.total.mutation.queue.max=256M
    accumulo> config -t usertable -s table.file.compress.type=snappy

On repeated data loads, the following commands may be helpful to re-set the state of the table quickly.

    accumulo> createtable tmp --copy-splits usertable --copy-config usertable
    accumulo> deletetable --force usertable
    accumulo> renametable tmp usertable
    accumulo> compact --wait -t accumulo.metadata

### 4. Load Data and Run Tests

Load the data:

    ./bin/ycsb load accumulo1.7 -s -P workloads/workloada \
         -p accumulo.zooKeepers=localhost \
         -p accumulo.columnFamily=ycsb \
         -p accumulo.instanceName=ycsb \
         -p accumulo.username=user \
         -p accumulo.password=supersecret \
         > outputLoad.txt

Run the workload test:

    ./bin/ycsb run accumulo1.7 -s -P workloads/workloada  \
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

