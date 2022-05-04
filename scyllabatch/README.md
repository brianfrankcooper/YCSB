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
        -p cassandra.username=cassandra -p cassandra.password=cassandra \
        -p scylla.hosts=ip1,ip2,ip3,...

Use as following:

    $ bin/ycsb run scylla -s -P workloads/workloada \
        -target 120000 -threads 840 -p recordcount=1000000000 \
        -p fieldcount=10 -p fieldlength=128 \
        -p operationcount=50000000 \
        -p scylla.username=cassandra -p scylla.password=cassandra \
        -p scylla.hosts=ip1,ip2,ip3,...

## On choosing meaningful configuration

### 1. Load target

Suppose, you want to test how a database handles an OLTP load.

In this case, to get the performance picture you want to look at the latency
distribution and utilization at the sustained throughput that is independent
of the processing speed. This kind of system called an open-loop system.
Use the `-target` flag to state desired requests arrival rate.

For example `-target 120000` means that we expect YCSB workers to generate
120,000 requests per second (RPS, QPS or TPS) overall to the database.

Why is this important? First, we want to look at the latency at some sustained
throughput target, not visa versa. Second, without a throughput target,
the system+loader pair will converge to the closed-loop system that has completely
different characteristics than what we wanted to measure. The load will settle
at the system equilibrium point. You will be able to find the throughput that will depend
on the number of loader threads (workers) but not the latency - only service time.
This is not something we expected.

For more information check out these resources on the coordinated omission problem.

See
[[1]](http://highscalability.com/blog/2015/10/5/your-load-generator-is-probably-lying-to-you-take-the-red-pi.html)
[[2]](https://medium.com/@siddontang/the-coordinated-omission-problem-in-the-benchmark-tools-5d9abef79279)
[[3]](https://bravenewgeek.com/tag/coordinated-omission/)
and [[this]](https://www.youtube.com/watch?v=lJ8ydIuPFeU)
great talk by Gil Tene.

### 2. Latency correction

To measure latency, it is not enough to just set a target. 
The latencies must be measured with the correction as we apply
a closed-class loader to the open-class problem. This is what YCSB
calls an Intended operation.

Intended operations have points in time when they were intended to be executed
according to the scheduler defined by the load target (--target). We must correct
measurement if we did not manage to execute an operation in time.

The fair measurement consists of the operation latency and its correction
to the point of its intended execution. Even if you don’t want to have
a completely fair measurement, use “both”:

    -p measurement.interval=both

Other options are “op” and “intended”. “op” is the default.

Another flag that affects measurement quality is the type of histogram
“-p measurementtype” but for a long time, it uses “hdrhistogram” that 
must be fine for most use cases.

### 3. Latency percentiles and multiple loaders

Latencies percentiles can't be averaged. Don't fall into this trap.
Neither averages nor p99 averages do not make any sense.

If you run a single loader instance look for P99 - 99 percentile.
If you run multiple loaders dump result histograms with:

    -p measurement.histogram.verbose=true

or 

    -p hdrhistogram.fileoutput=true
    -p hdrhistogram.output.path=file.hdr

merge them manually and extract required percentiles out of the
joined result.

Remember that running multiple workloads may distort original
workloads distributions they were intended to produce.

### 4. Parallelism factor and threads

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

    threads = K * shards per node * nodes

    for i3.4xlarge where

        - K is parallelism factor:

          K >= Target Throughput / QPS per Worker / Shards per node / Nodes / Workers per shard >= 1
          where
          Target Throughput = --target
          QPS per Worker = 1000 [ms/second] / Latency in ms expected at target Percentile
          Shards per node = vCPU per cluster node - 2
          Nodes = a number of nodes in the cluster.
          Workers per shard = Target Throughput / Shards per node / Nodes / QPS per Worker

        - Nodes is number of nodes in the cluster.

Another concern is that for high throughput scenarios you would probably
want to keep shards incoming queues non-empty. For that your parallelism factor
must be at least 2.

### 5. Number of connections

If you use original Cassandra drivers you need to pick the proper number
of connections per host. Scylla drivers do not require this to be configured
and by default create a connection per shard. For example if your node has
16 vCPU and thus 14 shards Scylla drivers will pick to create 14 connections
per host. An excess of connections may result in degraded latency.

Database client protocol is asynchronous and allows queueing requests in
a single connection. The default queue limit for local keys is 1024 and 256
for remote ones. Current binding implementation do not require this.

Both `scylla.coreconnections` and `scylla.maxconnections` define limits per node.
When you see `-p scylla.coreconnections=14 -p scylla.maxconnections=14` that means
14 connections per node.

Pick the number of connections per host to be divisible by the number of _shards_.

### 6. Other considerations

Consistency levels do not change consistency model or its strongness.
Even with `-p scylla.writeconsistencylevel=ONE` the data will be written
according to the number of a table replication factor (RF). Usually,
by default RF = 3. By using `-p scylla.writeconsistencylevel=ONE` you
can omit waiting all replicas to write the value. It will improve your
latency picture a bit but would not affect utilization.

Remember that you can't measure CPU utilization with Scylla by normal
Unix tools. Check out Scylla own metrics to see real reactors utilization.

For best performance it is crucial to evenly load all available shards.

### 7. Expected performance target

You can expect about 12500 uOPS / core (shard), where uOPS are basic
reads and writes operations post replication. Don't forget that usually
`Core = 2 vCPU` for HT systems.

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

* `scylla.local_dc`
  - Specify local datacenter for multi-dc setup.
  - By default uses LOCAL_QUORUM consistency level.  
  - Default value is empty.

- `scylla.lwt`
  - Use LWT for operations
  - Default is false.
