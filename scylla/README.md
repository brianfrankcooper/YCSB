<!--
Copyright (c) 2020 YCSB contributors. All rights reserved.

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

# Scylla CQL binding

Binding for [Scylla](https://www.scylladb.com/), using the CQL API
via the [Scylla driver](https://github.com/scylladb/java-driver/).

Requires JDK8.

## Creating a table for use with YCSB

For keyspace `ycsb`, table `usertable`:

    cqlsh> create keyspace ycsb
        WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
    cqlsh> USE ycsb;
    cqlsh> create table usertable (
        y_id varchar primary key,
        field0 varchar,
        field1 varchar,
        field2 varchar,
        field3 varchar,
        field4 varchar,
        field5 varchar,
        field6 varchar,
        field7 varchar,
        field8 varchar,
        field9 varchar);

**Note that `replication_factor` and consistency levels (below) will affect performance.**

## Quick start

Create a keyspace, and a table as mentioned above. Load data with:

    $ bin/ycsb load scylla -s -P workloads/workloada \
        -threads 84 -p recordcount=1000000000 \
        -p readproportion=0 -p updateproportion=0 \
        -p fieldcount=10 -p fieldlength=128 \
        -p insertstart=0 -p insertcount=1000000000 \
        -p cassandra.coreconnections=14 -p cassandra.maxconnections=14 \
        -p cassandra.username=cassandra -p cassandra.password=cassandra \
        -p scylla.hosts=ip1,ip2,ip3,...

Use as following:

    $ bin/ycsb run scylla -s -P workloads/workloada \
        -target 120000 -threads 840 -p recordcount=1000000000 \
        -p fieldcount=10 -p fieldlength=128 \
        -p operationcount=50000000 \
        -p scylla.coreconnections=280 -p scylla.maxconnections=280 \
        -p scylla.username=cassandra -p scylla.password=cassandra \
        -p scylla.hosts=ip1,ip2,ip3,... \
        -p scylla.tokenaware=true

## On choosing meaningful configuration

### 1. Load target

You want to test how a database handles load. To get the performance picture
you would want to look at the latency distribution and utilization under the
constant load. To select load use `-target` to state desired throughput level.

For example `-target 120000` means that we expect YCSB workers to generate
120,000 requests per second (RPS, QPS or TPS) to the database.

Why is this important? Because without setting target throughput you will be
looking only on the system equilibrium point that in the face of constantly
varying latency will not allow you to see either throughput, nor latency.

For more information check out these resources on the coordinated omission problem:

See
[[1]](http://highscalability.com/blog/2015/10/5/your-load-generator-is-probably-lying-to-you-take-the-red-pi.html)
[[2]](https://medium.com/@siddontang/the-coordinated-omission-problem-in-the-benchmark-tools-5d9abef79279)
[[3]](https://bravenewgeek.com/tag/coordinated-omission/)
and [[this]](https://www.youtube.com/watch?v=lJ8ydIuPFeU)
great talk by Gil Tene.

### 2. Parallelism factor and threads

Scylla utilizes [thread-per-core](https://www.scylladb.com/product/technology/) architecture design.
That means that a Node consists of shards that are mapped to the CPU cores 1-per-core.

In production setup, Scylla reserves 1 core to the interrupts handling and other system stuff.
For the system with hyper threading (HT) it means 2 virtual cores. From that follows that the number
of _Shards_ per _Node_ typically is `Number Of Cores - 2` for HT machine and
`Number Of Cores - 1` for a machine without HT.

It makes sense to select number of YCSB worker _threads_ to be multiple of the number
of shards, and the number of nodes in the cluster. For example:

    AWS Amazon i3.4xlarge has 16 vCPU (8 physical cores with HT).

    =>

    scylla node shards = vCPUs - 2 = 16 - 2 = 14

    =>

    threads = K * shards * nodes = K * 14 * nodes

    for i3.4xlarge where

        - K is parallelism factor >= 1,
        - Nodes is number of nodes in the cluster.

For example for 3 nodes `i3.4xlarge` and `-threads 840` means
`K = 20`, `shards = 14`, and `threads = 14 * 20 * 3`.

Thus, the `K` - the parallelism factor must be selected in the first order. If you
don't know what you want out of it start with 1.

For picking desired parallelism factor it is useful to come from desired `target`
parameter. It is better if the `target` is a multiple of `threads`.

Another concern is that for high throughput scenarios you would probably
want to keep shards incoming queues non-empty. For that your parallelism factor
must be at least 2.

### 3. Number of connections

Both `scylla.coreconnections` and `scylla.maxconnections` define limits
per node. When you see `-p scylla.coreconnections=280 -p scylla.maxconnections=280`
that means 280 connections per node.

Number of connections must be a multiple of:

- number of _shards_
- parallelism factor `K`

For example, for `i3.4xlarge` that has 14 shards per node and `K = 20`
it makes sense to pick `connections = shards * K = 14 * 20 = 280`.

### 4. Other considerations

Consistency levels do not change consistency model or its strongness.
Even with `-p scylla.writeconsistencylevel=ONE` the data will be written
according to the number of a table replication factor (RF). Usually,
by default RF = 3. By using `-p scylla.writeconsistencylevel=ONE` you
can omit waiting all replicas to write the value. It will improve your
latency picture a bit but would not affect utilization.

Remember that you can't measure CPU utilization with Scylla by normal
Unix tools. Check out Scylla own metrics to see real reactors utilization.

Always use [token aware](https://www.scylladb.com/2019/03/27/best-practices-for-scylla-applications/)
load balancing `-p scylla.tokenaware=true`.

For best performance it is crucial to evenly load all available shards.

### 5. Expected performance target

You can expect about 12500 uOPS / core (shard), where uOPS are basic
reads and writes operations post replication. Don't forget that usually
`Core = 2 * vCPU` for HT systems.

For example if we insert a row with RF = 3 we can count at least 3 writes -
1 write per each replica. That is 1 Transaction = 3 u operations.

Formula for evaluating performance with respect to workloads is:

    uOPS / vCPU = [
        Transactions * Writes_Ratio * Replication_Factor +
        Transactions * Read_Ratio   * Read_Consistency_level
        ] / [ (vCPU_per_node - 2) * (nodes) ]

    where Transactions == `-target` parameter (target throughput).

For example for _workloada_ that is 50/50 reads and writes for a cluster
of 3 nodes of i3.4xlarge (16 vCPU per node) and target of 120000 is:

    [ 120K * 0.5 * 3 + 120K * 0.5 * 2 (QUORUM) ] / [ (16 - 2) * 3 nodes ] =
    = 7142 uOPS / vCPU ~ 14000 uOPS / Core.

## Scylla configuration parameters

- `scylla.hosts` (**required**)
  - A list of Scylla nodes to connect to.
  - No default. Usage: `-p scylla.hosts=ip1,ip2,ip3,...`

* `scylla.port`

  - CQL port for communicating with Scylla cluster.
    This port must be the same on all cluster nodes.
  - Default is `9042`.

- `scylla.keyspace`

  - keyspace name - must match the keyspace for the table created (see above).
    See https://docs.scylladb.com/getting-started/ddl/#create-keyspace-statement for details.
  - Default value is `ycsb`

- `scylla.username` and `scylla.password`

  - Optional user name and password for authentication.
  - See https://docs.scylladb.com/operating-scylla/security/enable-authorization/ for details.

* `scylla.readconsistencylevel`
* `scylla.writeconsistencylevel`

  * Default value is `QUORUM`
  - Consistency level for reads and writes, respectively. 
    See the [Scylla documentation](https://docs.scylladb.com/glossary/#term-consistency-level-any) for details.

* `scylla.maxconnections`
* `scylla.coreconnections`

  * Defaults for max and core connections can be found here:
    https://github.com/scylladb/java-driver/tree/latest/manual/pooling.

* `scylla.connecttimeoutmillis`
* `scylla.readtimeoutmillis`
* `scylla.useSSL`

  * Default value is false.
  - To connect with SSL set this value to true.

* `scylla.tracing`
  * Default is false
  * https://docs.scylladb.com/using-scylla/tracing/

- `scylla.tokenaware`
  - Enable token awareness
  - Default value is false.

- `scylla.tokenaware_local_dc`
  - Restrict Round Robin child policy with the local dc nodes
  - Default value is empty.

- `scylla.lwt`
  - Use LWT for operations
  - Default is false.
