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

# YugabyteCQL binding
This driver enables YCSB to work with Yugabyte DB using CQL.

## Getting Started
### 1. Start your database
Start the database using steps mentioned here: https://docs.yugabyte.com/latest/deploy/manual-deployment/

### 2. Set up YCSB
Clone the YCSB project:

```
git clone https://github.com/yugabyte/YCSB.git && cd YCSB
```

Compile the code:
```
mvn clean package
```

We can also just compile the yugabyteCQL binding using:
```
mvn -pl yugabyteCQL -am clean package
```

### 3. Configure your database and table.
Create the Database and table using the cqlsh tool.

```
bin/cqlsh <ip> --execute "create keyspace ycsb"
bin/cqlsh <ip> --keyspace ycsb --execute 'create table usertable (y_id varchar primary key, field0 varchar, field1 varchar, field2 varchar, field3 varchar, field4 varchar, field5 varchar, field6 varchar,  field7 varchar, field8 varchar, field9 varchar);'
```

### 4. Configure YCSB connection properties
You need to set the following connection configurations in yugabyteCQL/db.properties:

```sh
hosts=127.0.0.1
port=9042
cassandra.username=yugabyte
```
### 5. Running the workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load yugabyteCQL -P yugabyteCQL/db.properties -P workloads/workloada
```

Then, you can run the workload:

```sh
bin/ycsb run yugabyteCQL -P yugabyteCQL/db.properties -P workloads/workloada
```

## Other Configuration Parameters

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
