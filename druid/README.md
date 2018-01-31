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
## Installing Druid

**WARNING:** This binding currently only explicitly supports Druid version 0.7.x.
Version 0.6.x and lower are not expected to work, higher versions *may* work, but have not been tested.

There is a tutorial on setting up druid available on their [official homepage][druid-tutorial]

Druid depends on Zookeeper to coordinate the cluster.

[druid-tutorial]: http://druid.io/docs/0.7.0/Tutorial%3A-A-First-Look-at-Druid.html

## YCSB Integration

The YCSB-Binding is not quite finished yet, so the properties required to run it are subject to change.

Additional options include:

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

 ## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load druid -P workload/tsworkloada -P druid.properties
bin/ycsb run druid -P workload/tsworkloada -P druid.properties
```
