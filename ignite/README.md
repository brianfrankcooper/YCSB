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

1.2 Copy Ignite configs:
    
    cp YCSB/ignite/resources/ignite.xml path/to/apache-ignite-fabric-**ignite_version**-bin
    cp YCSB/ignite/resources/ignite-sql.xml path/to/apache-ignite-fabric-**ignite_version**-bin

NOTE: Pay attention that some parameters such us ****_storagePath_****, ****_walPath_****, ****_walArchivePath_****
     should be overwritten by certain pathes. Also please add ip addresses of your host(s) inside the bean ****_TcpDiscoveryVmIpFinder_****

1.3 Copy ignite-binding-**YCSB version**-SNAPSHOT.jar to Ignite libs: 
    
    cp YCSB/ignite/target/ignite-binding-**YCSB_version**-SNAPSHOT.jar path/to/apache-ignite-fabric-**ignite_version**-bin/libs

Note: Please use YCSB/ignite/resources/**ignite.xml** for running **IgniteClient** tests and **ignite-sql.xml** for
**IgniteSqlClient** tests. 

More information about Apache Ignite WAL (Write Ahead Log): https://apacheignite.readme.io/docs/write-ahead-log

1.4 Start ignite nodes:
 
    cd path/to/apache-ignite-fabric-**ignite_version**-bin
    bin/**ignite.sh** ignite.xml
or

    bin/**ignite.sh** ignite-sql.xml

### 3. Load Data and Run Tests

Load the data:

    cd path/to/YCSB
    bin/ycsb load ignite -p hosts="10.0.0.1"
        -s -P workloads/workloada \
        -threads 4 \
        -p operationcount=100000 \
        -p recordcount=100000 \
         > outputload.txt
Note: '10.0.0.1' is ip address of one of hosts where was started Apache Ignite nodes.


Run the workload test with ignite:

    cd path/to/YCSB
    bin/ycsb run ignite -p hosts="10.0.0.1"
         -s -P workloads/workloada \
         -threads 4 \
         -p operationcount=100000 \
         -p recordcount=100000 \
          > output-ignite.txt

Run the workload test with ignite-sql:

    cd path/to/YCSB
    bin/ycsb run ignite-sql -p hosts="10.0.0.1"
         -s -P workloads/workloada \
         -threads 4 \
         -p operationcount=100000 \
         -p recordcount=100000 \
          > output-ignite-sql.txt
