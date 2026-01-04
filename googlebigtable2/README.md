<!--
Copyright (c) 2024 YCSB contributors. All rights reserved.

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

This driver provides a YCSB workload binding for Google's hosted Bigtable, the inspiration for a number of key-value stores like HBase and Cassandra. The Bigtable Java client provides both an idiomatic java and HBase client APIs. This binding implements the idiomatic java API for testing the native client. To test Bigtable using the HBase API, see the `hbase1` binding.
Please note, that this driver replaces googlebigtable driver which used a deprecated API.

## Quickstart

### 1. Setup a Bigtable Instance

Login to the Google Cloud Console and follow the [Creating Instance](https://cloud.google.com/bigtable/docs/creating-instance) steps. Make a note of your instance ID and project ID.

### 2. Launch the Bigtable Shell

From the Cloud Console, launch a shell and follow the [Quickstart](https://cloud.google.com/bigtable/docs/quickstart) up to step 4 where you install .

### 3. Create a Table

For best results, use the pre-splitting strategy recommended in [HBASE-4163](https://issues.apache.org/jira/browse/HBASE-4163):

```
PROJECT=<PROJECT_ID>
INSTANCE=<INSTANCE>
FAMILY=cf
SPLITS=$(echo 'num_splits = 200; puts (1..num_splits).map {|i| "user#{1000+i*(9999-1000)/num_splits}"}.join(",")' | ruby)
cbt -project $PROJECT -instance=$INSTANCE createtable usertable families=$FAMILY:maxversions=1 splits=$SPLITS
```

Make a note of the column family, in this example it's `cf``.

### 4. Download JSON Credentials

Follow these instructions for [Generating a JSON key](https://cloud.google.com/bigtable/docs/installing-hbase-shell#service-account) and save it to your host.

### 5. Load a Workload

Switch to the root of the YCSB repo and choose the workload you want to run and `load` it first. With the CLI you must provide the column family and instance properties to load.

```sh
GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_JSON_KEY> \
  ./bin/ycsb load googlebigtable2 \
  -p googlebigtable2.project=$PROJECT -p googlebigtable2.instance=$INSTANCE -p googlebigtable2.family=cf \
  -P workloads/workloada

```

Make sure to replace the variables in the angle brackets above with the proper value from your instance. Additional configuration parameters are available below.

The `load` step only executes inserts into the datastore. After loading data, run the same workload to mix reads with writes.

```sh
GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_JSON_KEY> \
  bin/ycsb run googlebigtable2 \
  -p googlebigtable2.project=$PROJECT -p googlebigtable2.instance=$INSTANCE -p googlebigtable2.family=cf \
  -P workloads/workloada

```

## Configuration Options

The following options can be configured using CLI (using the `-p` parameter).

* `googlebigtable2.project`: (Required) The ID of a Bigtable project.
* `googlebigtable2.instance`: (Required) The name of a Bigtable instance.
* `googlebigtable2.app-profile`: (Optional) The app profile to use.
* `googlebigtable2.family`: (Required) The Bigtable column family to target.
* `debug`: If true, prints debug information to standard out. The default is false.
* `googlebigtable2.use-batching`: (Optional) Whether or not to use client side buffering and batching of write operations. This can significantly improve performance and defaults to true.
* `googlebigtable2..max-outstanding-bytes`: (Optional) When batching is enabled, override the limit of number of outstanding mutation bytes.
* `googlebigtable2.reverse-scans`: (Optional) When enabled, scan start keys will be treated as end keys
* `googlebigtable2.timestamp`: (Optional) When set, the timestamp will be used for all mutations, avoiding unbounded growth of cell versions.
* `googlebigtable2.channel-pool-size`: (Optional) When set, disables channel pool autosizing and statically sets the pool size.

## Bigtable client version

As of this writing, Cloud Bigtable releases a new version of the client every 2 weeks. Newer client
versions will have performance optimizations not present in the version referenced by YCSB. However
when invoking ycsb using maven >= 3.9.0, the end user can override the Cloud Bigtable client version
via the `MAVEN_ARGS` environment variable. Please note, currently, the oldest version of the client
that this driver supports is 2.37.0 (released 2024/03/27).

Here are a couple of examples:

```sh
# Force version 2.47.0 of Cloud Bigtable client (released 2024/11/13
GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_JSON_KEY> \
MAVEN_ARGS="-Dgooglebigtable2.version=2.47.0" \
  bin/ycsb run googlebigtable2 \
  -p googlebigtable2.project=$PROJECT -p googlebigtable2.instance=$INSTANCE -p googlebigtable2.family=cf \
  -P workloads/workloada

# Use the latest version of Cloud Bigtable client (released 2024/11/13
GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_JSON_KEY> \
MAVEN_ARGS="-Dgooglebigtable2.version=RELEASE" \
  bin/ycsb run googlebigtable2 \
  -p googlebigtable2.project=$PROJECT -p googlebigtable2.instance=$INSTANCE -p googlebigtable2.family=cf \
  -P workloads/workloada
```
