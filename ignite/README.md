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

### 1. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:ignite-binding -am clean package

### 2. Start Apache Ignite
1.1 Download latest binary [Apache Ignite release](https://ignite.apache.org/download.cgi#binaries)

1.2 Start ignite nodes using apache-ignite-fabric-2.5.0-bin/bin/**ignite.sh** ignite.xml

Note: Please use YCSB/ignite/resources/**ignite.xml** for running **IgniteClient** tests and **ignite-sql.xml** for 
**IgniteSqlClient** tests. Pay attention that some parameters such us **storagePath**, ****_walPath_****, ****_walArchivePath_**** 
should be overwritten by certain pathes. Also please add ip addresses of your host inside the bean **TcpDiscoveryVmIpFinder**

More information about Apache Ignite WAL (Write Ahead Log): https://apacheignite.readme.io/docs/write-ahead-log
### 3. Load Data and Run Tests

Load the data:

    .bin/ycsb load ignite -p hosts="10.0.0.1" 
        -s -P workloads/workloada \
        -threads 4 \
        -p operationcount=100000 \ 
        -p recordcount=100000 \ 
         > outputload.txt 
Note: '10.0.0.1' is ip address of one of hosts where was started Apache Ignite nodes.

Run the workload test with IgniteClient:

    .bin/ycsb run ignite -p hosts="10.0.0.1" 
         -s -P workloads/workloada \
         -threads 4 \
         -p operationcount=100000 \ 
         -p recordcount=100000 \ 
          > outputload.txt

Run the workload test with IgniteSqlClient:

    .bin/ycsb run ignite-sql -p hosts="10.0.0.1" 
         -s -P workloads/workloada \
         -threads 4 \
         -p operationcount=100000 \ 
         -p recordcount=100000 \ 
          > outputload.txt