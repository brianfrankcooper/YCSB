<!--
Copyright (c) 2017 YCSB contributors. All rights reserved.

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

# Cloud Spanner Driver for YCSB

This driver provides a YCSB workload binding for Google's Cloud Spanner database, the first relational database service that is both strongly consistent and horizontally scalable. This binding is implemented using the official Java client library for Cloud Spanner which uses GRPC for making calls.

For best results, we strongly recommend running the benchmark from a Google Compute Engine (GCE) VM.

## Running a Workload

We recommend reading the [general guidelines](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload) in the YCSB documentation, and following the Cloud Spanner specific steps below.

### 1. Set up Cloud Spanner with the Expected Schema

Follow the [Quickstart instructions](https://cloud.google.com/spanner/docs/quickstart-console) in the Cloud Spanner documentation to set up a Cloud Spanner instance, and create a database with the following schema:

```
CREATE TABLE usertable (
  id STRING(MAX),
  field0 STRING(MAX),
  field1 STRING(MAX),
  field2 STRING(MAX),
  field3 STRING(MAX),
  field4 STRING(MAX),
  field5 STRING(MAX),
  field6 STRING(MAX),
  field7 STRING(MAX),
  field8 STRING(MAX),
  field9 STRING(MAX),
) PRIMARY KEY(id);
```
Make note of your project ID, instance ID, and database name.

### 2. Set Up Your Environment and Auth

Follow the [set up instructions](https://cloud.google.com/spanner/docs/getting-started/set-up) in the Cloud Spanner documentation to set up your environment and authentication. When not running on a GCE VM, make sure you run `gcloud auth application-default login`.

### 3. Edit Properties

In your YCSB root directory, edit `cloudspanner/conf/cloudspanner.properties` and specify your project ID, instance ID, and database name.

### 4. Run the YCSB Shell

Start the YCBS shell connected to Cloud Spanner using the following command:

```
./bin/ycsb shell cloudspanner -P cloudspanner/conf/cloudspanner.properties
```

You can use the `insert`, `read`, `update`, `scan`, and `delete` commands in the shell to experiment with your database and make sure the connection works. For example, try the following:

```
insert name field0=adam
read name field0
delete name
```

### 5. Load the Data

You can load, say, 10 GB of data into your YCSB database using the following command:

```
./bin/ycsb load cloudspanner -P cloudspanner/conf/cloudspanner.properties -P workloads/workloada -p recordcount=10000000 -p cloudspanner.batchinserts=1000 -threads 10 -s
```

We recommend batching insertions so as to reach ~1 MB of data per commit request; this is controlled via the `cloudspanner.batchinserts` parameter which we recommend setting to `1000` during data load.

If you wish to load a large database, you can run YCSB on multiple client VMs in parallel and use the `insertstart` and `insertcount` parameters to distribute the load as described [here](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload-in-Parallel). In this case, we recommend the following:

* Use ordered inserts via specifying the YCSB parameter `insertorder=ordered`;
* Use zero-padding so that ordered inserts are actually lexicographically ordered; the option `zeropadding = 12` is set in the default `cloudspanner.properties` file;
* Split the key range evenly between client VMs;
* Use few threads on each client VM, so that each individual commit request contains keys which are (close to) consecutive, and would thus likely address a single split; this also helps avoid overloading the servers.

The idea is that we have a number of 'write heads' which are all writing to different parts of the database (and thus talking to different servers), but each individual head is writing its own data (more or less) in order. See the [best practices page](https://cloud.google.com/spanner/docs/best-practices#loading_data) for further details.

### 6. Run a Workload

After data load, you can a run a workload, say, workload B, using the following command:

```
./bin/ycsb run cloudspanner -P cloudspanner/conf/cloudspanner.properties -P workloads/workloadb -p recordcount=10000000 -p operationcount=1000000 -threads 10 -s 
```

Make sure that you use the same `insertorder` (i.e. `ordered` or `hashed`) and `zeropadding` as specified during the data load. Further details about running workloads are given in the [YCSB wiki pages](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).

## Configuration Options

In addition to the standard YCSB parameters, the following Cloud Spanner specific options can be configured using the `-p` parameter or in `cloudspanner/conf/cloudspanner.properties`.

* `cloudspanner.database`: (Required) The name of the database created in the instance, e.g. `ycsb-database`.
* `cloudspanner.instance`: (Required) The ID of the Cloud Spanner instance, e.g. `ycsb-instance`.
* `cloudspanner.project`: The ID of the project containing the Cloud Spanner instance, e.g. `myproject`. This is not strictly required and can often be automatically inferred from the environment.
* `cloudspanner.readmode`: Allows choosing between the `read` and `query` interface of Cloud Spanner. The default is `query`.
* `cloudspanner.batchinserts`: The number of inserts to batch into a single commit request. The default value is 1 which means no batching is done. Recommended value during data load is 1000.
* `cloudspanner.boundedstaleness`: Number of seconds we allow reads to be stale for. Set to 0 for strong reads (default). For performance gains, this should be set to 10 seconds.
