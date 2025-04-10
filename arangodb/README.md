<!--
Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on ArangoDB. 

### 1. Start ArangoDB
See https://docs.arangodb.com/Installing/index.html

### 2. Set Up YCSB

Download the [latest YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest) file. Follow the instructions.

### 3. Run YCSB

Now you are ready to run! First, drop the existing collection: "usertable" under database "ycsb":
	
	db._collection("usertable").drop()

Then, load the data:

    ./bin/ycsb load arangodb -s -P workloads/workloada -p arangodb.ip=xxx -p arangodb.port=xxx

Then, run the workload:

    ./bin/ycsb run arangodb -s -P workloads/workloada -p arangodb.ip=xxx -p arangodb.port=xxx

See the next section for the list of configuration parameters for ArangoDB.

## ArangoDB Configuration Parameters

- `arangodb.ip`
  - Default value is `localhost`

- `arangodb.port`
  - Default value is `8529`.

- `arangodb.protocol`
  - Default value is 'VST'

- `arangodb.waitForSync`
  - Default value is `true`.
  
- `arangodb.transactionUpdate`
  - Default value is `false`.

- `arangodb.dropDBBeforeRun`
  - Default value is `false`.

For more infos take a look into the official [ArangoDB Java Driver Docs](https://www.arangodb.com/docs/stable/drivers/java-reference-setup.html#network-protocol). Note that very old versions of ArangoDB (i.e. 3.0) require settings the protocol to "HTTP_JSON".
