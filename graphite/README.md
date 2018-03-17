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

## Installing Graphite

Graphite has an extensive [installation guide][install-guide] you should consult.
The binding uses Carbon to insert data and the render-api over HTTP to retrieve the data.

[install-guide]: https://graphite.readthedocs.io/en/latest/install.html

## YCSB Integration

The binding requires the following properties to be set:

- `ip` **hostname or ip**:
 The IP or Hostname that your gnocchi installation is available under.

- `plaintextPort` **carbon plaintext endpoint port**:
 The port that Carbon is configured to listen on for data writes.
 Default is most likely 2003.

- `graphiteApiPort` **render-api http endpoint port**:
 The port that Graphite is configured to serve API requests on.
 Default is most likely 80.

Additional options include:

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load graphite -P workload/tsworkloada -P graphite.properties
bin/ycsb run graphite -P workload/tsworkloada -P graphite.properties
```
