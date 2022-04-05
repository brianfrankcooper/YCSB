<!--
Copyright (c) 2022 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on DAOS. 

### 1. Start DAOS.

Refer https://docs.daos.io/ for more information.

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:daos-binding -am clean package

### 4. Provide DAOS Connection Parameters

The following connection parameters are available.

  * `daos.pool` - The DAOS pool to connect to
  * `daos.cont` - The DAOS container to use

    Note:    
      Setting both daos.pool and daos.cont parameters are required for the benchmark to run.    
      Refer DAOS admin guide for pool creation and the user guide for the container creation.     
      While creating the DAOS container, set the redundancy factor accordingly.    

Add the connection parameters to the workload or set them with the shell command, as in:

    ./bin/ycsb load daos -s -P workloads/workloada -p daos.pool=tank -p daos.cont=mycont >outputLoad.txt

### 5. Load Data and Run Tests

Load the data:

    ./bin/ycsb load daos -s -P workloads/workloada >outputLoad.txt

Run the workload test:

    ./bin/ycsb run daos -s -P workloads/workloada >outputRun.txt

