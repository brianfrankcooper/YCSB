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

## Installing KairosDB

KairosDB currently runs with Java 1.8 or later.
The full installation instructions can be found at the [official documentation][official-docs]

### Steps:

 1. Download the tarball from the [official releases][releases]
 2. Extract to where KairosDB is intended to be run from
 3. Adapt `conf/kairosdb.properties` to use the datastore you wish to use.  
    The default of an in-memory H2 database is slow.
 4. Make sure that `$JAVA_HOME` is set correctly
 5. change to the `bin` directory under the extracted directory and run `./kairosdb.sh run`

## YCSB Integration

The binding requires the following properties to be set:

 - `port` **kairosdb REST API port**:
  The port that your kairosdb installation exposes it's rest API on.

 - `ip` **kairosdb hostname**:
  The hostname that kairosdb is reachable under.

Additional tuning options include:

 - `debug` **enable debug logging**:
  Verbose debug information is logged by the ycsb-binding.

 - `test` **test run**:
  If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load kairosdb -P workload/tsworkloada -P kairosdb.properties
bin/ycsb run kairosdb -P workload/tsworkloada -P kairosdb.properties
```

 [official-docs]: https://kairosdb.github.io/docs/build/html/GettingStarted.html#install
 [releases]: https://github.com/kairosdb/kairosdb/releases