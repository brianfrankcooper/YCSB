<!--
Copyright (c) 2016 YCSB contributors. All rights reserved.
Copyright 2014 Basho Technologies, Inc.

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

Riak KV Client for Yahoo! Cloud System Benchmark (YCSB)
=======================================================

The Riak KV YCSB client is designed to work with the Yahoo! Cloud System Benchmark (YCSB) project (https://github.com/brianfrankcooper/YCSB) to support performance testing for the 2.x.y line of the Riak KV database.

Creating a <i>bucket type</i> to use with YCSB
----------------------------

Perform the following operations on your Riak cluster to configure it for the benchmarks.

Set the default backend for Riak to <i>LevelDB</i> in the `riak.conf` file of every node of your cluster. This is required to support <i>secondary indexes</i>, which are used for the `scan` transactions. You can do this by modifying the proper line as shown below.

```
storage_backend = leveldb
```

Now, create a bucket type named "ycsb"<sup id="a1">[1](#f1)</sup> by logging into one of the nodes in your cluster. Then, to use the <i>strong consistency model</i><sup id="a2">[2](#f2)</sup> (default), you need to follow the next two steps.

1. In every `riak.conf` file, search for the `##strong_consistency=on` line and uncomment it. It's important that you do this <b>before you start your cluster</b>!
2. Run the following `riak-admin` commands:

  ```
  riak-admin bucket-type create ycsb '{"props":{"consistent":true}}'
  riak-admin bucket-type activate ycsb
  ```

Note that when using the strong consistency model, you **may have to specify the number of replicas to create for each object**. The *R* and *W* parameters (see next section) will in fact be ignored. The only information needed by this consistency model is how many nodes the system has to successfully query to consider a transaction completed. To set this parameter, you can add `"n_val":N` to the list of properties shown above (by default `N` is set to 3).

If instead you want to use the <i>eventual consistency model</i> implemented in Riak, then type: 
```
riak-admin bucket-type create ycsb '{"props":{"allow_mult":"false"}}'
riak-admin bucket-type activate ycsb
```

Riak KV configuration parameters
----------------------------
You can either specify these configuration parameters via command line or set them in the `riak.properties` file.

* `riak.hosts` - <b>string list</b>, comma separated list of IPs or FQDNs. For example: `riak.hosts=127.0.0.1,127.0.0.2,127.0.0.3` or `riak.hosts=riak1.mydomain.com,riak2.mydomain.com,riak3.mydomain.com`.
* `riak.port` - <b>int</b>, the port on which every node is listening. It must match the one specified in the `riak.conf` file at the line `listener.protobuf.internal`.
* `riak.bucket_type` - <b>string</b>, it must match the name of the bucket type created during setup (see section above).
* `riak.r_val` - <b>int</b>, this value represents the number of Riak nodes that must return results for a read operation before the transaction is considered successfully completed. 
* `riak.w_val` - <b>int</b>, this value represents the number of Riak nodes that must report success before an insert/update transaction is considered complete.
* `riak.read_retry_count` - <b>int</b>, the number of times the client will try to read a key from Riak.
* `riak.wait_time_before_retry` - <b>int</b>, the time (in milliseconds) before the client attempts to perform another read if the previous one failed.
* `riak.transaction_time_limit` - <b>int</b>, the time (in seconds) the client waits before aborting the current transaction.
* `riak.strong_consistency` - <b>boolean</b>, indicates whether to use *strong consistency* (true) or *eventual consistency* (false).
* `riak.debug` - <b>boolean</b>, enables debug mode. This displays all the properties (specified or defaults) when a benchmark is started. Moreover, it shows error causes whenever these occur.

<b>Note</b>: For more information on workloads and how to run them please see: https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload

<b id="f1">1</b> As specified in the `riak.properties` file.  See parameters configuration section for further info. [↩](#a1)

<b id="f2">2</b> <b>IMPORTANT NOTE:</b> Currently the `scan` transactions are <b>NOT SUPPORTED</b> for the benchmarks which use the strong consistency model! However this will not cause the benchmark to fail, it simply won't perform any scan transaction at all. These latter will immediately return with a `Status.NOT_IMPLEMENTED` code.  [↩](#a2)
