<!--
Copyright (c) 2012 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on OrientDB running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/nuvolabase/YCSB.git
    cd YCSB
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load orientdb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run orientdb -s -P workloads/workloada

See the next section for the list of configuration parameters for OrientDB.

## OrientDB Configuration Parameters

### `OrientDB.url` (default: `local:C:/temp/databases/ycsb`)

### `OrientDB.user` (default `admin`)

### `OrientDB.password` (default `admin`)
