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

#### 1. Set Up YCSB

Clone the YCSB source code from git repository:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB

#### 2. Install GridDB 4.0 CE and place the "gridstore.jar" file (Java Client) under the following directory

    griddb/lib/

Please download [GridDB 4.0 CE](https://github.com/griddb/griddb_nosql/releases/tag/v4.0.0).

To see "How to install GridDB with RPM", please refer to [RPM Installation Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_RPM_InstallGuide.html).

### Build

Run following command

    $ mvn -pl com.yahoo.ycsb:griddb-binding -am clean package

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

