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

# Kudu bindings for YCSB

[Kudu](http://getkudu.io) is a storage engine that enables fast analytics on fast data.

## Benchmarking Kudu

Use the following command line to load the initial data into an existing Kudu cluster with default
configurations.

```
bin/ycsb load kudu -P workloads/workloada
```

Additional configurations:
* `kudu_master_addresses`: The master's address. The default configuration expects a master on localhost.
* `kudu_pre_split_num_tablets`: The number of tablets (or partitions) to create for the table. The default
uses 4 tablets. A good rule of thumb is to use 5 per tablet server.
* `kudu_table_num_replicas`: The number of replicas that each tablet will have. The default is 3. Should
only be configured to use 1 instead, for single node tests.
* `kudu_sync_ops`: If the client should wait after every write operation. The default is true.
* `kudu_block_size`: The data block size used to configure columns. The default is 4096 bytes.

Then, you can run the workload:

```
bin/ycsb run kudu -P workloads/workloada
```

## Using a previous client version

If you wish to use a different Kudu client version than the one shipped with YCSB, you can specify on the
command line with `-Dkudu.version=x`. For example:

```
mvn -pl com.yahoo.ycsb:kudu-binding -am package -DskipTests -Dkudu.version=0.7.1
```

Note that prior to 1.0, Kudu doesn't guarantee wire or API compability between versions and only the latest
one is officially supported.
