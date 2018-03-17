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
## Installing Axibase TSDB

The full installation instructions are available from the [atsd downloads page][atsd-downloads]
There are container images for multiple container platforms as well as packages for most major Linux distributions.

[atsd-downloads]: https://github.com/axibase/atsd/blob/master/installation/README.md

## YCSB Integration

You must define the following properties for the ycsb-binding to work:

- `httpPort`:
 The port that your atsd instance exposes it's REST-Endpoint on.
 In a default atsd configuration this should be 8088.

- `tcpPort`:
 The port that your atsd instance exposes it's TCP-Command endpoint on.
 In a default atsd configuration this should be 8081.

- `ip`:
 The hostname / IP of the remote server that your atsd instance is running on.
 Must be a valid http hostname for an `http://ip:port/` specification.

- `username`:
 The username to use for authentication against atsd.

- `password`:
 The password to use for authentication against atsd.

Additional options include:

- `debug` **enable debug logging**:
 If set to true, the client will set the highest log-level in the java language bindign for influxdb.
 Additionally verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

 ## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load atsd -P workload/tsworkloada -P influxdb.properties
bin/ycsb run atsd -P workload/tsworkloada -P influxdb.properties
```
