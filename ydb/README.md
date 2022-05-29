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

## Quick start

Binding for [YDB](https://www.ydb.tech/), using SQL API
via the [YDB Java SDK](https://github.com/yandex-cloud/ydb-java-sdk).

You might also want to use [YCSB-helpers](https://github.com/eivanov89/YCSB-helpers).

This section describes how to run YCSB on YDB.

### 1. Start YDB

Install and start YDB. The database must be running, you don't need any preparation steps.
By default YCSB will create table named `usertable` with all required fields.

### 2. Install Java and Maven


### 3. Set Up YCSB

Run the following command to build:

  > mvn -pl site.ycsb:ydb-binding -am clean package

Don't forget to unpack the built package somewhere. To avoid extra maven invocations run commands from this instruction inside unpacked package, not source tree.

### 4. Run YCSB

Load the data:

    $ ./bin/ycsb load ydb -s -P workloads/workloada \
        -p database=/Root/db1 \
        -p endpoint=grpc://SOME_YDB_HOST:2135 \
        -p dropOnInit=true > outputLoad.txt

Run the workload:

    $ ./bin/ycsb run ydb -s -P workloads/workloada \
        -p database=/Root/db1 \
        -p endpoint=grpc://SOME_YDB_HOST:2135  > outputLoad.txt

## YDB Configuration Parameters

- `endpoint`
 - This should be an endpoint for YDB database, e.g. `grpc://some.host.net:2135`.
 - No default value, parametr is mandatory.

- `database`
 - Full path to the database, e.g. `/home/mydb`.
 - No default value, parametr is mandatory.

- `dropOnInit`
 - During initialization table `usertable` will be dropped if parameter set to `true`
 - Default is `false`

- `dropOnClean`
 - At the end table `usertable` will be dropped if parameter set to `true`
 - Default is `false`