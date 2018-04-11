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

This section describes how to run YCSB with Sparksee. 

### 1. Set Up YCSB with Sparksee

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:sparksee-binding -am clean package

### 2. Run YCSB with Sparksee
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load sparksee -s -P workloads/workloada -p sparksee.path=/my/path/to/db.gdb

Then, run the workload:

    ./bin/ycsb run sparksee -s -P workloads/workloada -p sparksee.path=/my/path/to/db.gdb

For further configuration see below: 

### Default Configuration Parameters
The default settings for Sparksee are as follows:
	
- `sparksee.path`
  - This sets the path for the database **file**. The file has to have the ending `.gdb`.
  - Default: `sparkseeDB.gdb`
    - This file will be created in the current directory.

- `sparksee.logLevel`
  - This sets the log level for the sparksee client.
  - Available values are:
    - `Off`
    - `Info`
    - `Fine`
    - `Warning`
    - `Debug`
    - `Severe`
    - `Config`
  - Default: `Off`