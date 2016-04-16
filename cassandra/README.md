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
# THIS BINDING IS DEPRECATED
----------------------------
Date of removal from YCSB: **March 2016**

Due to the low amount of use and support for older Cassandra lineages (0.X and 1.X), YCSB will not support clients for these versions either.

For Cassandra 2.X use the ```cassandra2-cql``` client: https://github.com/brianfrankcooper/YCSB/tree/master/cassandra2.

# Cassandra (0.7, 0.8, 1.x) drivers for YCSB

**For Cassandra 2 CQL support, use the `cassandra2-cql` binding.  The Thrift drivers below are deprecated, and the CQL driver here does not support Cassandra 2.1+.**

There are three drivers in the Cassandra binding:

* `cassandra-7`: Cassandra 0.7 Thrift binding.
* `cassandra-8`: Cassandra 0.8 Thrift binding.
* `cassandra-10`: Cassandra 1.0+ Thrift binding.
* `cassandra-cql`: Cassandra CQL binding, for Cassandra 1.x to 2.0. See `cassandra2/README.md` for details on parameters.

# `cassandra-10`

## Creating a table

Using `cassandra-cli`:

      create keyspace usertable with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:1};

      create column family data with column_type = 'Standard' and comparator = 'UTF8Type';

**Note that `replication_factor` and consistency levels (below) will affect performance.**

## Configuration Parameters

- `hosts` (**required**)
  - Cassandra nodes to connect to.
  - No default.

* `port`
  - Thrift port for communicating with Cassandra cluster.
  * Default is `9160`.

- `cassandra.columnfamily`
  - Column family name - must match the column family for the table created (see above).
  - Default value is `data`

- `cassandra.username`
- `cassandra.password`
  - Optional user name and password for authentication. See http://docs.datastax.com/en/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html for details.

* `cassandra.readconsistencylevel`
* `cassandra.scanconsistencylevel`
* `cassandra.writeconsistencylevel`

  - Default value is `ONE`
  - Consistency level for reads and writes, respectively. See the [DataStax documentation](http://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html) for details.
  - *Note that the default setting does not provide durability in the face of node failure. Changing this setting will affect observed performance.* See also `replication_factor`, above.
