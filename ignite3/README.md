<!--
Copyright (c) 2021 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on [Apache Ignite 3](https://ignite.apache.org).

### 1. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/gridgain/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:ignite3-binding -am clean package

### 2. Start Apache Ignite
1.1 Download latest binary [Apache Ignite 3 release](https://ignite.apache.org/download.cgi#binaries)

1.2 Start ignite3 node(s)
 
### 3. Load Data and Run Tests

` python bin/ycsb run ignite3 -p hosts="127.0.0.1" -s -P ./workloads/workloada -threads 4 -p operationcount=1000000 -p recordcount=1000000 -p measurementtype=timeseries -p dataintegrity=true`
