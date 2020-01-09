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

This section describes how to run YCSB on RADOS of Ceph.

### 1. Start RADOS

After you start your Ceph cluster, check your cluster’s health first. You can check on the health of your cluster with the following:

    ceph health

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

You can compile only RADOS-binding, EG:

    mvn -pl site.ycsb:rados-binding -am clean package

You can skip the test, EG:

    mvn -pl site.ycsb:rados-binding -am clean package -DskipTests

### 4. Configuration Parameters

- `rados.configfile`
  - The path of the Ceph configuration file
  - Default value is '/etc/ceph/ceph.conf'

- `rados.id`
  - The user id to access the RADOS service
  - Default value is 'admin'

- `rados.pool`
  - The pool name to be used for benchmark
  - Default value is 'data'

You can set configurations with the shell command, EG:

    ./bin/ycsb load rados -s -P workloads/workloada -p "rados.configfile=/etc/ceph/ceph.conf" -p "rados.id=admin" -p "rados.pool=data" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load rados -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run rados -s -P workloads/workloada > outputRun.txt
