<!--
Copyright (c) 2014 - 2016 YCSB contributors. All rights reserved.

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

## Quick Start

This section describes how to run YCSB on Apache Geode.

### Get Apache Geode

You can download Geode from https://geode.apache.org/releases/

#### Start Geode Cluster

Use the Geode shell (gfsh) to start the cluster. You will need to start
at-least one locator which is a member discovery service and one or more
Geode servers.

Launch gfsh:

```
$ cd $GEODE_HOME
$ ./bin/gfsh
```

Start a locator and two servers:

```
gfsh> start locator --name=locator1
gfsh> configure pdx --read-serialized=true
gfsh> start server --name=server1 --server-port=40404
gfsh> start server --name=server2 --server-port=40405
```

Create the "usertable" region required by YCSB driver:
```
gfsh>create region --name=usertable --type=PARTITION
```
gfsh has tab autocompletion, so you can play around with various options.

### Start YCSB workload

From your YCSB directory, you can run the ycsb workload as follows
```
./bin/ycsb load geode -P workloads/workloada -p geode.locator=host[port]
```
(default port of locator is 10334).

In the default mode, ycsb geode driver will connect as a client to the geode
cluster. To make the ycsb driver a peer member of the distributed system
use the property
`-p geode.topology=p2p -p geode.locator=host[port]`

YCSB uses geode 1.2.0, which should be compatible with all later
versions of the geode server. To make YCSB run with a different version of the
geode client, you can change the geode.version property in pom.xml.

Note:
For update workloads, please use the property `-p writeallfields=true`
