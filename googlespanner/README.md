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

# Google Spanner Driver for YCSB

## Limitations and caveats

* Only Application Default Credentials are supported. In other words,
  if running on a Google Cloud instance with permissions granted when
  creating it, authentication will seamlessly work. But use of
  explicit keys is not implemented.
* The primary key is not escaped in the implementation, and so the workload
  generator must not generate keys containing characters not safe to put
  within single-quotes in Spanner SQL.
* Manual creation of Spanner instances, databases and tables is required.

## How to use

### Create the database

Create the appropriate database and tables through your preferred
means, such as the Google Cloud console. Create an instance, a
database and a table (naming is free).

The table *must* contain a primary key column by the name `pkey`, and
necessary fields. The necessary fields are dictated by the
workload. If using the core workload, these fields will be named
`field0`, `field1`, ..., `fieldN` (by default, N=9). For other
workloads, adjust accordingly.

Both the primary key and all other columns should be strings.

### Configure your workload

At minimum, specify these properties in your workload configuration:

* `googlespanner.instance` - the name of your spanner instance
* `googlespanner.database` - the name of your database
* `table` - the name of the table you created

### Run it

No special process required other than specifying `googlespanner` as
the name of the binding. Example:

```
./bin/ycsb load googlespanner -P wload -threads 20
./bin/ycsb run googlespanner -P wload -threads 20 -target 200
```
