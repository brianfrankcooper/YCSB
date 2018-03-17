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
## Installing InfluxDB

Follow the installation instructions from [InfluxDB Downloads](http://portal.influxdata.com/download).
After installation you can start start influxdb either as a systemctl service using `systemctl start influxdb` or manually using `/usr/bin/influxd`.

By default influxdb will listen to incoming HTTP requests on port 8086.
You can adjust the query listening port in the configuration under `/etc/influxdb/influxdb.conf`.
Up-to-date documentation can be found at [docs.influxdata.com](https://docs.influxdata.com/influxdb/v1.4/)

## YCSB Integration

The influxdb-binding will automatically create a database and retention policy for you if necessary.
Currently the binding does not support configuring the retention policy settings.

You must define the following properties for the ycsb-binding to work:
 
- `port`:  
 The port that your influxdb instance exposes it's REST-Endpoint on.
 In a default influxdb configuration this should be 8086
 
- `dbName`:  
 The name of the database to use for querying.
 If it exists, it will be truncated.
 At the start of the testrun data will be truncated from the database.
 The ycsb-binding intentionally does not truncate the data generated after the testrun to enable examining resulting data.
 
- `ip`:  
 The hostname / IP of the remote server that your influxdb instance is running on.
 Must be a valid http hostname for an `http://ip:port/` specification. 

Additional options include:

- `retentionPolicy` **custom retention policy name**:  
 Set this to a retention policy that you previously configured on the database `[dbName]` to enforce using that retention policy.
 If this is not set, the retention policy defaults to the default automatically generated policy (`autogen`).

- `batch` **enable client batching**:  
 If set to true, the client will bundle requests in packages of 10, or send all currently buffered requests after a second.
 This trades latency for I/O, enable this when you want to increase the throughput of requests / sec at the cost of a bit of initial response lag.
 
- `debug` **enable debug logging**:  
 If set to true, the client will set the highest log-level in the java language bindign for influxdb.
 Additionally verbose debug information is logged by the ycsb-binding.
 
- `test` **test run**:  
 If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.
 
 ## YCSB Runs
 
The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.
 
```bash
bin/ycsb load influxdb -P workload/tsworkloada -P influxdb.properties
bin/ycsb run influxdb -P workload/tsworkloada -P influxdb.properties
```
