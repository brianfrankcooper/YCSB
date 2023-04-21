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
        -p dsn=grpc://SOME_YDB_HOST:2135/Root/db1 \
        -p dropOnInit=true \
        -p import=true \
        -threads 64

Run the workload:

    $ ./bin/ycsb run ydb -s -P workloads/workloada \
        -p dsn=grpc://SOME_YDB_HOST:2135/Root/db1 \
        -threads 64

Please use `-threads` option depending on YDB cluster size.

## YDB Configuration Parameters

- `dsn`
  - This should be a data source name (dsn) for YDB database, e.g. `grpc://some.host.net:2135/home/mydb`.
  - No default value, parametr is mandatory.

- `keyColumnName`
  - Key column name
  - Default is `id`

- `token`
  - token used for auth, otherwise environment auth will be used

- `dropOnInit`
  - During initialization table `usertable` will be dropped (might not exists) if parameter set to `true` and new one will be created.
  - Default is `false`

- `dropOnClean`
  - At the end table `usertable` will be dropped if parameter set to `true`
  - Default is `false`

- `maxparts`
  - Maximum number of partitions, see [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT](https://ydb.tech/en/docs/concepts/datamodel#auto_partitioning_max_partitions_count)
  - Default is `50`

- `maxpartsizeMB`
  - Maximum size of partition in MB before split
  - Default is `2000` (2 GB)

- `preparedInsertUpdateQueries`
  - Use prepared queries to update/insert
  - Default is `true`

- `insertInflight`
  - Allow insert() to return OK before completing to have inflight > 1
  - default `1`

- `presplitTable`
  - When create table, set initial key ranges to speedup parallel load
  - default `false`

- `import`
  - Shortcut for "forceUpsert=true, bulkUpsert=true, insertInflight=10, bulkUpsertBatchSize=500, presplitTable=true"
  - default `false`

- `forceUpsert`
  - Both insert() and update() use upsert, i.e. blind writes
  - default `no`

- `forceUpdate`
  - update() by default uses upsert, i.e. blind writes, with this option it switches to update statement
  - default `no`

- `bulkUpsert`
  - upsert uses bulk iterface, can be combined with `forceUpsert` to speedup load phase
  - default `no`

- `bulkUpsertBatchSize`
  - size of bulk upsert batch, must be used with `bulkUpsert`
  - default 100

- `compression`
  - Use compression for all columns. Must specify a storage device type for the data in this column group, i.e. "ssd", "hdd" or "nvme".
  - default ``, i.e. no compression

- `splitByLoad`
  - Split parts when they're overloaded, see [AUTO_PARTITIONING_BY_LOAD](https://ydb.tech/en/docs/concepts/datamodel#auto_partitioning_by_load)
  - default `true`

- `splitBySize`
  - Split parts when they're overloaded, see [AUTO_PARTITIONING_BY_SIZE](https://ydb.tech/en/docs/concepts/datamodel#auto_partitioning_by_size)
  - default `true`