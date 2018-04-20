<!--
Copyright (c) 2016 - 2018 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Neo4J running locally.

### 1. Set Up YCSB

First, clone the YCSB git repository:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB

You can now compile the project:

    mvn clean package

If you want to only compile the Neo4J binding, use:

    mvn -pl com.yahoo.ycsb:neo4j-binding -am clean package

### 2. DB creation with Neo4J

By default, the client will create the Neo4J database in a folder named `'neo4j.db'`.
To specify another folder, use the `db.path=path/to/database` property.

## 3. Run YCSB

First, load the data:

    ./bin/ycsb load neo4j -s -P workloads/workloada -p db.path=path/to/database

Then, run the workload:

    ./bin/ycsb run neo4j -s -P workloads/workloada -p db.path=path/to/database

## Neo4J Configuration Parameters

* `db.path` - Folder which will store the Neo4J database

## Known Issues

* There is a performance issue around the scan operation. The Neo4J Java API can't find the nodes to scan, thus it is needed to use a Cypher language query. It is slower than the embedded Java API.
