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

# AsyncHBase Driver for YCSB

This driver provides a YCSB workload binding for Apache HBase using an alternative to the included HBase client. AsyncHBase is completely asynchronous for all operations and is particularly useful for write heavy workloads. Note that it supports a subset of the HBase client APIs but supports all public released versions of HBase.

## Quickstart

### 1. Setup Hbase

Follow directions 1 to 3 from ``hbase098``'s readme.

### 2. Load a Workload

Switch to the root of the YCSB repo and choose the workload you want to run and `load` it first. With the CLI you must provide the column family at a minimum if HBase is running on localhost. Otherwise you must provide connection properties via CLI or the path to a config file. Additional configuration parameters are available below.

```
bin/ycsb load asynchbase -p columnfamily=cf -P workloads/workloada

```

The `load` step only executes inserts into the datastore. After loading data, run the same workload to mix reads with writes.

```
bin/ycsb run asynchbase -p columnfamily=cf -P workloads/workloada

```

## Configuration Options

The following options can be configured using CLI (using the `-p` parameter) or via a JAVA style properties configuration file.. Check the [AsyncHBase Configuration](http://opentsdb.github.io/asynchbase/docs/build/html/configuration.html) project for additional tuning parameters.

* `columnfamily`: (Required) The column family to target.
* `config`: Optional full path to a configuration file with AsyncHBase options.
* `hbase.zookeeper.quorum`: Zookeeper quorum list.
* `hbase.zookeeper.znode.parent`: Path used by HBase in Zookeeper. Default is "/hbase".
* `debug`: If true, prints debug information to standard out. The default is false.
* `clientbuffering`: Whether or not to use client side buffering and batching of write operations. This can significantly improve performance and defaults to true.
* `durable`: When set to false, writes and deletes bypass the WAL for quicker responses. Default is true.
* `jointimeout`: A timeout value, in milliseconds, for waiting on operations synchronously before an error is thrown.
* `prefetchmeta`: Whether or not to read meta for all regions in the table and connect to the proper region servers before starting operations. Defaults to false.


Note: This module includes some Google Guava source files from version 12 that were later removed but are still required by HBase's test modules for setting up the mini cluster during integration testing.