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

## Installing Prometheus

This guide is based on [the official prometheus documentation][official-docs]

### Steps

 1. Download & Extract the latest Prometheus release for your platform.
 2. Configure Prometheus by adapting the `prometheus.yml` shipped with the release.
 3. Start prometheus, passing your adapted configuration file as argument like so:

     ```bash
     ./prometheus --config.file=prometheus.yml
     ```

 ## YCSB Integration 

The binding requires the following properties to be set:

 - `ipPrometheus` **the hostname that prometheus is reachable under**
 - `portPrometheus` **the port that prometheus exposes the metrics under**
 - `ipPushgateway` **the hostname that the push gateway for data is reachable under**
 - `portPushgateway` **the port that the push gateway is listening on**

Additional tuning options include:

 - `plainTextFormat` **use the plaintext format for insertions**:
    The Prometheus Push gateway supports a plaintext format for inserations.
    This switch controls whether to use that or not.
    Defaults to `true`.

 - `useCount` **enable count aggregation**:
    This switch controls whether to actually count during scan queries that request a COUNT aggregation.
    If this is disabled, the minimum is used instead.
    Defaults to `true`.

 - `debug` **enable debug logging**:
  Verbose debug information is logged by the ycsb-binding.

 - `test` **test run**:
  If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load prometheus -P workload/tsworkloada -P prometheus.properties
bin/ycsb run prometheus -P workload/tsworkloada -P prometheus.properties
```

 [official-docs]: https://prometheus.io/docs/introduction/first_steps/