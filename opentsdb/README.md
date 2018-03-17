t<!--
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
## Installing OpenTSDB

Follow the installation instructions from [the OpenTSDB documentation][install-opentsdb].
OpenTSDB requires Java 1.6 or higher, HBase 0.92 or later and GnuPlot 4.2 or later.
It's intended for Linux systems, but can be run on Windows, if built manually.

Before first usage, OpenTSDB needs a few tables to be setup on your HBase installation.
This can be accomplished by running the `create_table.sh` script with the environment variables `COMPRESSION` and `HBASE_HOME` set.
The tables `tsdb`, `tsdb-uid`, `tsdb-tree` and `tsdb-meta` will be created in the HBase instance.

[install-opentsdb]: http://opentsdb.net/docs/build/html/installation.html

## Configuring OpenTSDB

The [User documentation][user-docs] contains extensive documentation on how to [configure your OpenTSDB installation][configure-opentsdb].

[user-docs]: http://opentsdb.net/docs/build/html/user_guide/index.html
[configure-opentsdb]: http://opentsdb.net/docs/build/html/user_guide/configuration.html

## YCSB Integration

You must define the following properties for the ycsb-binding to work:
 
- `port`:  
 The port that your opentsdb instance exposes it's REST-Endpoint on.
 In the default configuration that port is 4242.
 
- `ip`:  
 The hostname / IP of the remote server that your opentsdb instance is running on.
 Must be a valid http hostname for an `http://ip:port/` specification. 

Additional options include:

- `queryUrl`:  
 The path-specification for the query-endpoint of your opentsdb instance.
 Defaults to `/api/query`

- `putUrl`:  
 The path specification for the put-endpoint of your opentsdb instance.
 Defaults to `/api/put`

- `filterForTags` **filter for tags**:  
 Versions of OpenTSDB above 2.2 support filtering by tags.
 For lower versions, this should be set to false.
 Use tags in the workload for actually filtering data.
 If set to false, tags will not be applied as filters to the query in any way.

- `useCount` **use count for aggregation**:  
 Versions of OpenTSDB above 2.2 support count.
 For lower versions, this should be set to false, to drop COUNT aggregation requests to MIN.

- `useMs` **use millisecond timestamps**:  
 Enables millisecond resolution of all queries.
 Defaults to true.
 Otherwise the resolution of queries is seconds.

- `debug` **enable debug logging**:  
 Verbose debug information is logged by the ycsb-binding.
 
- `test` **test run**:  
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.
 
 ## YCSB Runs
 
The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load opentsdb -P workload/tsworkloada -P opentsdb.properties
bin/ycsb run opentsdb -P workload/tsworkloada -P opentsdb.properties
```
