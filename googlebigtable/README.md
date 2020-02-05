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

# Google Bigtable  Driver for YCSB

This driver provides a YCSB workload binding for Google's hosted Bigtable, the inspiration for a number of key-value stores like HBase and Cassandra. The Bigtable Java client provides both Protobuf based GRPC and HBase client APIs. This binding implements the Protobuf API for testing the native client. To test Bigtable using the HBase API, see the `hbase1` binding.

## Quickstart

### 1. Setup a Bigtable Instance

Login to the Google Cloud Console and follow the [Creating Instance](https://cloud.google.com/bigtable/docs/creating-instance) steps. Make a note of your instance ID and project ID.

### 2. Launch the Bigtable Shell

From the Cloud Console, launch a shell and follow the [Quickstart](https://cloud.google.com/bigtable/docs/quickstart) up to step 4 where you launch the HBase shell.

### 3. Create a Table

For best results, use the pre-splitting strategy recommended in [HBASE-4163](https://issues.apache.org/jira/browse/HBASE-4163):

```
hbase(main):001:0> n_splits = 200 # HBase recommends (10 * number of regionservers)
hbase(main):002:0> create 'usertable', 'cf', {SPLITS => (1..n_splits).map {|i| "user#{1000+i*(9999-1000)/n_splits}"}}
```

Make a note of the column family, in this example it's `cf``.

### 4. Download JSON Credentials

Follow these instructions for [Generating a JSON key](https://cloud.google.com/bigtable/docs/installing-hbase-shell#service-account) and save it to your host.

### 5. Load a Workload

Switch to the root of the YCSB repo and choose the workload you want to run and `load` it first. With the CLI you must provide the column family and instance properties to load.

```
bin/ycsb load googlebigtable -p columnfamily=cf -p google.bigtable.project.id=<PROJECT_ID> -p google.bigtable.instance.id=<INSTANCE> -p google.bigtable.auth.json.keyfile=<PATH_TO_JSON_KEY> -P workloads/workloada

```

Make sure to replace the variables in the angle brackets above with the proper value from your instance. Additional configuration parameters are available below.

The `load` step only executes inserts into the datastore. After loading data, run the same workload to mix reads with writes.

```
bin/ycsb run googlebigtable -p columnfamily=cf -p google.bigtable.project.id=<PROJECT_ID> -p google.bigtable.instance.id=<INSTANCE> -p google.bigtable.auth.json.keyfile=<PATH_TO_JSON_KEY> -P workloads/workloada

```

## Configuration Options

The following options can be configured using CLI (using the `-p` parameter) or hbase-site.xml (add the HBase config directory to YCSB's class path via CLI). Check the [Cloud Bigtable Client](https://github.com/manolama/cloud-bigtable-client) project for additional tuning parameters.

* `columnfamily`: (Required) The Bigtable column family to target.
* `google.bigtable.project.id`: (Required) The ID of a Bigtable project.
* `google.bigtable.instance.id`: (Required) The name of a Bigtable instance.
* `google.bigtable.auth.service.account.enable`: Whether or not to authenticate with a service account. The default is true.
* `google.bigtable.auth.json.keyfile`: (Required) A service account key for authentication.
* `debug`: If true, prints debug information to standard out. The default is false.
* `clientbuffering`: Whether or not to use client side buffering and batching of write operations. This can significantly improve performance and defaults to true.
