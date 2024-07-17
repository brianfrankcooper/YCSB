<!--
Copyright (c) 2024 YCSB contributors. All rights reserved.

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

# Apache Cassandra >= 2.1 CQL binding

Binding for [Apache Cassandra](http://cassandra.apache.org), using the CQL API
via the [DataStax driver](https://docs.datastax.com/en/developer/java-driver/4.17/manual/index.html)

To run against the (deprecated) Cassandra Thrift API, use the `cassandra-10` binding.

## Creating a table for use with YCSB

For keyspace `ycsb`, table `usertable`, noting you should apply your own replication,
compaction and other settings based on your testing configuration.
```sql
CREATE KEYSPACE ycsb WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor': 3
};
```

```sql
CREATE TABLE ycsb.usertable (
    y_id VARCHAR PRIMARY KEY,
    field0 VARCHAR,
    field1 VARCHAR,
    field2 VARCHAR,
    field3 VARCHAR,
    field4 VARCHAR,
    field5 VARCHAR,
    field6 VARCHAR,
    field7 VARCHAR,
    field8 VARCHAR,
    field9 VARCHAR
);
```

**Note that parameters like `replication_factor`, `compaction` etc. will affect performance.**

## Cassandra Configuration Parameters

* `cassandra.driverconfig` (**required**)
  * Path to a HOCON configuration for configuring the Cassandra driver.
  * See the reference configuration here <https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/reference/index.html>

* `cassandra.driverprofile.read`
  * Profile within the driver config to use for read queries.
  * Driver configs have an anonymous config that all profiles inherit from (and you can modify this), which is used by default if this parameter is not specified

* `cassandra.driverprofile.scan`
  * Profile within the driver config to use for scan queries.
  * Driver configs have an anonymous config that all profiles inherit from (and you can modify this), which is used by default if this parameter is not specified

* `cassandra.driverprofile.insert`
  * Profile within the driver config to use for insert queries.
  * Driver configs have an anonymous config that all profiles inherit from (and you can modify this), which is used by default if this parameter is not specified

* `cassandra.driverprofile.update`
  * Profile within the driver config to use for update queries.
  * Driver configs have an anonymous config that all profiles inherit from (and you can modify this), which is used by default if this parameter is not specified

* `cassandra.driverprofile.delete`
  * Profile within the driver config to use for delete queries.
  * Driver configs have an anonymous config that all profiles inherit from (and you can modify this), which is used by default if this parameter is not specified

* `cassandra.tracing`
  * Default is `false`
  * Captures detailed information about the internal operations performed by all nodes in the cluster in order to build the response.
  * This is an expensive operation and should only be done on a few queries, adjusted by `cassandra.tracingfrequency`.

* `cassandra.tracingfrequency`
  * Default is `1000`
  * Determines how often tracing will be performed, i.e. for every `n` queries, tracing will be enabled on that query.
