<!--
Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.

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

# Couchbase Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Couchbase Server cluster. It uses the official Couchbase Java SDK and provides a rich set of configuration options.

## Quickstart

### 1. Start Couchbase Server
You need to start a single node or a cluster to point the client at. Please see [http://couchbase.com](couchbase.com) for more details and instructions.

### 2. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load couchbase -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run couchbase -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property (as seen in the next section) like this:

```
bin/ycsb run couchbase -s -P workloads/workloada -p couchbase.useJson=false
```

## Scans in the CouchbaseClient
The scan operation in the CouchbaseClient requires a Couchbase View to be created manually. To do this:

1. Go to the Couchbase UI, then to Views
2. Create a new development view, specify a ddoc and view name, use these in your YCSB properties. See Configuration Options below.
3. The default map code is sufficient.
4. Save, and publish this View.

## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and implement the facilities on your own.

You can set the following properties (with the default settings applied):

 - couchbase.url=http://127.0.0.1:8091/pools => The connection URL from one server.
 - couchbase.bucket=default => The bucket name to use.
 - couchbase.password= => The password of the bucket.
 - couchbase.checkFutures=true => If the futures should be inspected (makes ops sync).
 - couchbase.persistTo=0 => Observe Persistence ("PersistTo" constraint).
 - couchbase.replicateTo=0 => Observe Replication ("ReplicateTo" constraint).
 - couchbase.ddoc => The ddoc name used for scanning
 - couchbase.view => The view name used for scanning
 - couchbase.stale => How to deal with stale values in View Query for scanning. (OK, FALSE, UPDATE_AFTER)
 - couchbase.json=true => Use json or java serialization as target format.

