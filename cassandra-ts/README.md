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

# Apache Cassandra 2.x CQL binding for Time Series Workloads

Binding for [Apache Cassandra](http://cassandra.apache.org), using the CQL API
via the [DataStax
driver](http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/whatsNew2.html).  
This binding has been specifically developed for use with YCSB Time Series workloads (see TimeSeriesWorkload.java).

## Creating a table for use with YCSB Time Series Workloads

Assuming a keyspace of `ycsb`, and table `metrics`:

    cqlsh> create keyspace ycsb
        WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
    cqlsh> USE ycsb;
    cqlsh> create table metrics (
        metric text,
        tags text,
        valuetime timestamp,
        value double,
        PRIMARY KEY (metric, tags, valuetime))
        WITH CLUSTERING ORDER BY (tags ASC, valuetime ASC);

**Note that `replication_factor` and consistency levels (below) will affect performance.**

## Cassandra Configuration Parameters

- `hosts` (**required**)
  - Cassandra nodes to connect to.
  - No default.

* `port`
  * CQL port for communicating with Cassandra cluster.
  * Default is `9042`.

- `cassandra.keyspace`
  Keyspace name - must match the keyspace for the table created (see above).
  See http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html for details.

  - Default value is `ycsb`

- `cassandra.username`
- `cassandra.password`
  - Optional user name and password for authentication. See http://docs.datastax.com/en/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html for details.

* `cassandra.readconsistencylevel`
* `cassandra.writeconsistencylevel`

  * Default value is `ONE`
  - Consistency level for reads and writes, respectively. See the [DataStax documentation](http://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html) for details.
  * *Note that the default setting does not provide durability in the face of node failure. Changing this setting will affect observed performance.* See also `replication_factor`, above.

* `cassandra.maxconnections`
* `cassandra.coreconnections`
  * Defaults for max and core connections can be found here: https://datastax.github.io/java-driver/2.1.8/features/pooling/#pool-size. Cassandra 2.0.X falls under protocol V2, Cassandra 2.1+ falls under protocol V3.
* `cassandra.connecttimeoutmillis`
* `cassandra.useSSL`
  * Default value is false.
  - To connect with SSL set this value to true.
* `cassandra.readtimeoutmillis`
  * Defaults for connect and read timeouts can be found here: https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/SocketOptions.html.
* `cassandra.tracing`
  * Default is false
  * https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tracing_r.html