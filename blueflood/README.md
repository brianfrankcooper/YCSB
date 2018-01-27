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
## Blueflood Set Up and Dependencies

According to the ["10 Minute Guide"][10min-guide], blueflood can be set up with either Docker or Vagrant.

Blueflood runs against Java 8.
All it's data is stored in Cassandra.
Their ["Deployment Dependencies"][deployment-dependencies] wiki entry says that Cassandra versions 1.0, 1.1, 1.2, 2.0 and 2.1 are supported.

[10min-guide]: https://github.com/rackerlabs/blueflood/wiki/10-Minute-Guide
[deployment-dependencies]: https://github.com/rackerlabs/blueflood/wiki/Deployment-Dependencies


## YCSB Integration

**NOTE:** The YCSB-Binding currently **only supports** blueflood v2.0

You must define the following properties for the ycsb-binding to work:

- `ip`:
 The hostname / IP of the remote server that your blueflood instance is running on.
 Must be a valid http hostname for an `http://ip:port/` specification.

Additional options include:

- `tenantId` **id since blueflood is multi-tenant**:
 The tenant-id to use when connecting to the blueflood instance.
 If left empty, `usermetric` is used.

- `ingestPort` **write api port**:
 The port that insert operations are connecting to.
 Defaults to 19000.

- `queryPort` **query api port**:
 The port that all query operations are connecting to.
 Defaults to 19001.

- `retries`:
 Number of tries to perform when running requests against the database.
 Defaults to 3.

- `ttl`:
 The retention duration of data in the database in seconds.
 Defaults to a year.

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

 ## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load blueflood -P workload/tsworkloada -P blueflood.properties
bin/ycsb run blueflood -P workload/tsworkloada -P blueflood.properties
```
