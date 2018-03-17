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

## Installing Gnocchi

Gnocchi requires three external services to work correctly.
These services are:

 - Incoming Measure Storage
 - Aggregated Metric Storage
 - Index

All three external services are provided through interchangeable drivers.
As such any gnocchi installation will need at least an installation of either

 - PostgreSQL
 - MySQL >= 5.6.4

That way gnocchi stores incoming measures and aggregated metrics as files and the index in the database chosen.

Gnocchi itself can be installed using pip as follows:

```bash
pip install gnocchi
```

You should check the ["getting started"][getting-started] page of gnocchi to check exactly what installation variant you want.
In addition there's furhter information on hte ["Installation"][installation] page.

Note that the binding uses the **basic** authentication scheme supported without any additional plugins.

[getting-started]: https://gnocchi.xyz/intro.html
[installation]: https://gnocchi.xyz/install.html

## YCSB Integration

The binding uses the last available metric exposed under `/v1/metric`.
If no metric has been created yet, the binding automatically creates a metric named `"usermetric"` with the archive policy `"policy"`.

That archive policy has the following settings:
```json
{
    "aggregation_methods" : [ "sum", "mean", "count" ],
    "back_window" : 0,
    "definition" : [ { "points": 1000000, "granularity": "1s" } ]
}
```

The binding requires the following properties to be set:

- `ip` **hostname or ip**:
 The IP or Hostname that your gnocchi installation is available under.

- `port` **exposed port**:
 The Port that your gnocchi installation is exposed under.
 For a default installation this should default to 8041.

Additional options include:

- `debug` **enable debug logging**:
 Verbose debug information is logged by the ycsb-binding.

- `test` **test run**:
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load gnocchi -P workload/tsworkloada -P gnocchi.properties
bin/ycsb run gnocchi -P workload/tsworkloada -P gnocchi.properties
```
