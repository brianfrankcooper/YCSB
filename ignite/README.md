<!--
Copyright (c) 2018 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on [Apache Ignite](https://ignite.apache.org). 

### 1. Start Apache Ignite

https://apacheignite-sql.readme.io/docs

See the [Apache Ignite Documentation](https://apacheignite.readme.io/docs/getting-started)
for details on installing and running Apache Ignite.

Config _**YCSB/ignite/resources/config/ycsb.xml_**  should be used for the benchmarking.
Please pay attention that some parameters such us **_storagePath_**, **_walPath_**, **_walArchivePath_** should be 
overwritten ([Persistence store documentation](https://apacheignite.readme.io/docs/distributed-persistent-store))
Before start of IgniteSqlClient should be uncommented **_queryEntities_** part of config.


### 2. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:ignite-binding -am clean package

### 3. Load Data and Run Tests

Load the data:

    .bin/ycsb load ignite -p hosts="10.0.0.1" 
        -s -P workloads/workloada \
        -threads 4 \
        -p operationcount=100000 \ 
        -p recordcount=100000 \ 
         > outputload.txt 
Note: '10.0.0.1' is ip address of one of hosts where was started Apache Ignite.

Run the workload test:

    .bin/ycsb run ignite -p hosts="10.0.0.1" 
         -s -P workloads/workloada \
         -threads 4 \
         -p operationcount=100000 \ 
         -p recordcount=100000 \ 
          > outputload.txt

