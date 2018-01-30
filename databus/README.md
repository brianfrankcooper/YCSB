<!--
Copyright (c) 2018 YCSB contributors. All rights reserved.

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
## Installing Databus

Follow the installation instructions from the [Energy DataBus Documentation][databus-docs].

[databus-docs]: https://github.com/deanhiller/databus/tree/master/doc/DataBus%20Documentation

## YCSB Integration

The databus-binding will automatically create a database and retention policy for you if necessary.
Currently the binding does not support configuring the retention policy settings.

You must define the following properties for the ycsb-binding to work:

- `ip`:
 The hostname / ip that your databus instance is available under.

- `port`:
 The port that your databus instance exposes it's REST-Endpoint on.
 In a default databus configuration this should be 8080

- `user`:
 The username used by the internal HTTP Client to authenticate against the databus instance.

- `apiKey`:
 The API key used as password when authenticating the given user against the databus instance.

- `tableType`:
 One of `rtable`, `rtstable` or `tstable`, signalling to use the Relational, Relational Time Series or Time Series storage format for the table used in benchmarking.

Additional options include:

- `dbName`:
 The name of the database to use for querying.
 The ycsb-binding intentionally does not truncate the data generated after the testrun to enable examining resulting data.

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

 ## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

**NOTE:** as of now this binding only supports writing to the metric `usermetric` using exactly three tags (`TAG0`, `TAG1` and `TAG2`).

```bash
bin/ycsb load databus -P workload/tsworkloada -P databus.properties
bin/ycsb run databus -P workload/tsworkloada -P databus.properties
```
