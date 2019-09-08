<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

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

# YCSB Memcached binding

This section describes how to run YCSB on memcached.

## 1. Install and start memcached service on the host(s)

Debian / Ubuntu:

    sudo apt-get install memcached

RedHat / CentOS:

    sudo yum install memcached

## 2. Install Java and Maven

See step 2 in [`../mongodb/README.md`](../mongodb/README.md).

## 3. Set up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:memcached-binding -am clean package

## 4. Load data and run tests

Load the data:

    ./bin/ycsb load memcached -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run memcached -s -P workloads/workloada > outputRun.txt

## 5. memcached Connection Parameters

A sample configuration is provided in
[`conf/memcached.properties`](conf/memcached.properties).

### Required params

- `memcached.hosts`

  This is a comma-separated list of hosts providing the memcached interface.
  You can use IPs or hostnames. The port is optional and defaults to the
  memcached standard port of `11211` if not specified.

### Optional params

- `memcached.shutdownTimeoutMillis`

  Shutdown timeout in milliseconds.

- `memcached.objectExpirationTime`

  Object expiration time for memcached; defaults to `Integer.MAX_VALUE`.

- `memcached.checkOperationStatus`

  Whether to verify the success of each operation; defaults to true.

- `memcached.readBufferSize`

  Read buffer size, in bytes.

- `memcached.opTimeoutMillis`

  Operation timeout, in milliseconds.

- `memcached.failureMode`

  What to do with failures; this is one of `net.spy.memcached.FailureMode` enum
  values, which are currently: `Redistribute`, `Retry`, or `Cancel`.

- `memcached.protocol`
  Set to 'binary' to use memcached binary protocol. Set to 'text' or omit this field
  to use memcached text protocol

You can set properties on the command line via `-p`, e.g.:

    ./bin/ycsb load memcached -s -P workloads/workloada \
        -p "memcached.hosts=127.0.0.1" > outputLoad.txt
