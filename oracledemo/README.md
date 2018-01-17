<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Oracle NoSQL. 

### 1. Start Oracle NoSQL

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:oracledemo-binding -am clean package

### 4. Load Data and Run Tests using Oracle NoSQL Demo Connection Parameters
    
Provide the store and endpoint parameters with the shell commands.

Load the data:

    ./bin/ycsb load oracledemo -s -P workloads/workloada -p "oracledemo.store=kvstore" -p "oracledemo.endpoint=Oracle_NoSQL_DB_AD1_0:5000" -p "oracledemo.debug=false" > outputLoad.txt

Run the workload test:

    ./bin/ycsb run oracledemo -s -P workloads/workloada -p "oracledemo.store=kvstore" -p "oracledemo.endpoints=Oracle_NoSQL_DB_AD1_0:5000,Oracle_NoSQL_DB_AD2_0:5000,Oracle_NoSQL_DB_AD3_0:5000" -p "oracledemo.debug=false" -threads 36 > outputRun.txt

