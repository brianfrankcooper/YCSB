<!--
Copyright (c) 2012 - 2017 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on a MapR Cluster against MapR-DB (JSON). 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 2. Create MapR-DB JSON Table

    maprcli volume create -name tables -path /tables
    maprcli table create -path /tables/docTable -tabletype json

### 3. Run YCSB
    
###### 3.1. Load Table
	
	./bin/ycsb load maprjsondb -P workloads/workloada -cp $(mapr clientclasspath) -p table=/tables/docTable

###### 3.2. Run workload
	
	./bin/ycsb run maprjsondb -P workloads/workloadb -cp $(mapr clientclasspath) -p table=/tables/docTable
