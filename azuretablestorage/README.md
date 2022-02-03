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

This section describes how to run YCSB on Azure table storage. 

### 1. Create an Azure Storage account.
###    https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/#create-a-storage-account

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:azuretablestorage-binding -am clean package

### 4. Provide Azure Storage parameters
    
Set the account name and access key.

- `azure.account`
- `azure.key`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load azuretablestorage -s -P workloads/workloada -p azure.account=YourAccountName -p azure.key=YourAccessKey > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load azuretablestorage -s -P workloads/workloada -p azure.account=YourAccountName -p azure.key=YourAccessKey > outputLoad.txt

Run the workload test:

    ./bin/ycsb run azuretablestorage -s -P workloads/workloada -p azure.account=YourAccountName -p azure.key=YourAccessKey > outputRun.txt
	
### 6. Optional Azure Storage parameters

- `azure.batchsize`	
	Could be between 1 ~ 100. Insert records to table in batch if batchsize > 1.
- `azure.protocol`
	https(in default) or http.
- `azure.table`
	The name of the table('usertable' in default).
- `azure.partitionkey`
	The partitionkey('Test' in default).
- `azure.endpoint`
	For Azure stack WOSS.
	
EG:
    ./bin/ycsb load azuretablestorage -s -P workloads/workloada -p azure.account=YourAccountName -p azure.key=YourAccessKey -p azure.batchsize=100 -p azure.protocol=http
	
	

