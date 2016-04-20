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

Creating a <i>bucket-type</i> to use with YCSB
----------------------------

Perform the following operations on your Riak cluster to configure it for the benchmarks.

Set the default backend for Riak to <i>LevelDB</i> in the `riak.conf` file of every node of your cluster. This is required to support <i>secondary indexes</i>, which are used for the `scan` transactions. You can do this by modifying the proper line as shown below.

```
storage_backend = leveldb
```
After this, create a bucket type named "ycsb"<sup id="a1">[1](#f1)</sup> by logging into one of the nodes in your cluster. Now you're ready to set up the cluster to operate using one between strong and eventual consistency model as shown in the next two subsections. 

###Strong consistency model 

To use the <i>strong consistency model</i> (default), you need to follow the next two steps.

1. In every `riak.conf` file, search for the `##strong_consistency=on` line and uncomment it. It's important that you do this <b>before you start your cluster</b>!
2. Run the following `riak-admin` commands:

  ```
  riak-admin bucket-type create ycsb '{"props":{"consistent":true}}'
  riak-admin bucket-type activate ycsb
  ```

When using this model, you **may want to specify the number of replicas to create for each object**<sup id="a2">[2](#f2)</sup>: the *R* and *W* parameters (see next section) will in fact be ignored. The only information needed by this consistency model is how many nodes the system has to successfully query to consider a transaction completed. To set this parameter, you can add `"n_val":N` to the list of properties shown above (by default `N` is set to 3).

####A note on the scan transactions 
Currently, `scan` transactions are not _directly_ supported, as there is no suitable mean to perform them properly. This will not cause the benchmark to fail, it simply won't perform any scan transaction at all (these will immediately return with a `Status.NOT_IMPLEMENTED` code).

However, a possible workaround has been provided: considering that Riak doesn't allow strong-consistent bucket-types to use secondary indexes, we can create an eventually consistent one just to store (*key*, *2i indexes*) pairs. This will be later used only to obtain the keys where the objects are located, which will be then used to retrieve the actual objects from the strong-consistent bucket. If you want to use this workaround, then you have to create and activate a "_fake bucket-type_" using the following commands:
```
riak-admin bucket-type create fakeBucketType '{"props":{"allow_mult":"false","n_val":1,"dvv_enabled":false,"last_write_wins":true}}'
riak-admin bucket-type activate fakeBucketType
```
A bucket-type so defined isn't allowed to _create siblings_ (`allow_mult":"false"`), it'll have just _one replica_ (`"n_val":1`) which'll store the _last value provided_ (`"last_write_wins":true`) and _vector clocks_ will be used instead of _dotted version vectors_ (`"dvv_enabled":false`). Note that setting `"n_val":1` means that the `scan` transactions won't be much *fault-tolerant*, considering that if a node fails then a lot of them could potentially fail. You may indeed increase this value, but this choice will necessarily load the cluster with more work. So, the choice is yours to make!
Then you have to set the `riak.strong_consistent_scans_bucket_type` property (see next section) equal to the name you gave to the aforementioned "fake bucket-type" (e.g. `fakeBucketType` in this case).

Please note that this workaround involves a **double store operation for each insert transaction**, one to store the actual object and another one to save the corresponding 2i index. In practice, the client won't notice any difference, as the latter operation is performed asynchronously. However, the cluster will be obviously loaded more, and this is why the proposed "fake bucket-type" to create is as less _resource-demanding_ as possible.

###Eventual consistency model

If you want to use the <i>eventual consistency model</i> implemented in Riak, you have just to type: 
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
* `riak.strong_consistent_scans_bucket_type` - **string**, indicates the bucket-type to use to allow scans transactions when using strong consistency mode.
* `riak.debug` - <b>boolean</b>, enables debug mode. This displays all the properties (specified or defaults) when a benchmark is started. Moreover, it shows error causes whenever these occur.

<b>Note</b>: For more information on workloads and how to run them please see: https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload

<b id="f1">1</b> As specified in the `riak.properties` file.  See parameters configuration section for further info. [↩](#a1)

<b id="f2">2</b> More info about properly setting up a fault-tolerant cluster can be found at http://docs.basho.com/riak/kv/2.1.4/configuring/strong-consistency/#enabling-strong-consistency.[↩](#a2)

