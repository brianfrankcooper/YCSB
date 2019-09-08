<!--
Copyright (c) 2018 TOSHIBA Digital Solutions Corporation.
Copyright (c) 2018 YCSB contributors.

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

## Overview

GridDB (https://github.com/griddb/griddb_nosql) is a highly scalable NoSQL database best suited for IoT and Big Data.  

This is GridDB Binding for YCSB.

## Environment
Library building and program execution have been checked in the following environment.

    OS: CentOS 6.9(x64).
    Java: JDK 1.8.0_191
    Maven: 3.5.4
    Python: 2.6.6

## Quick start

### Preparations

Clone the YCSB source code from git repository:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB

### Build

Run following command

    $ mvn -pl site.ycsb:griddb-binding -am clean package

Then, some jar files are created in:

    griddb/target

### GridDB setup

Please set the number of cpu core as /dataStore/concurrency in gs_node.json and "32KB" as /dataStore/storeBlockSize in gs_cluster.json.

### Run YCSB

GridDB needs to be started in advance.  
Please execute our program with fieldcount=10 and fieldlength=100.  
First, load the data:

    ./bin/ycsb load griddb -P workloads/workloada
    -p notificationAddress=<GridDB notification address(default is 239.0.0.1)>
    -p notificationPort=<GridDB notification port(default is 31999)>
    -p clusterName=<GridDB cluster name>
    -p userName=<GridDB user name>
    -p password=<GridDB password>
    -p fieldcount=10
    -p fieldlength=100

Then, run the workload:

    ./bin/ycsb run griddb -P workloads/workloada
    -p notificationAddress=<GridDB notification address(default is 239.0.0.1)>
    -p notificationPort=<GridDB notification port(default is 31999)>
    -p clusterName=<GridDB cluster name>
    -p userName=<GridDB user name>
    -p password=<GridDB password>
    -p fieldcount=10
    -p fieldlength=100

