<!--
Copyright (c) 2014 - 2016 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on BangDB server

### 1. Start BangDB server
Start BangDB server with database set to "ycsb" in the server
bangdb config file

    BANGDB_DATABASE_NAME = "ycsb"

    

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:bangdb-binding -am clean package

### 4. Set parameters through bangdb.config file
    
Provide bangdb.config file (check the sample in YCSB/bangdb)
and move to YCSB directory

    mv YCSB/bangdb/bangdb.config ..

### 5. Load data and run tests

Do the following from YCSB directory
Load the data:

    ./bin/ycsb load bangdb -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run bangdb -s -P workloads/workloada > outputRun.txt

