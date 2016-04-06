Riak KV Client for Yahoo! Cloud System Benchmark (YCSB)
--------------------------------------------------------

The Riak KV YCSB client is designed to work with the Yahoo! Cloud System Benchmark (YCSB) project (https://github.com/brianfrankcooper/YCSB) to support performance testing for the 2.0.X line of the Riak KV database.

Creating a <i>bucket type</i> to use with YCSB
----------------------------

Perform the following operations on your Riak cluster to configure it for the benchmarks.

Set the default backend for Riak to <i>LevelDB</i> in `riak.conf` (required to support <i>secondary indexes</i> used for the <b>scan</b> workloads):

```
storage_backend = leveldb
```

Create a bucket type named "ycsb"<sup id="a1">[1](#f1)</sup> by logging into one of the nodes in your cluster. Then if you want to use the

* <i>default consistency model</i> (i.e. eventual), run the following riak-admin commands:

```
riak-admin bucket-type create ycsb '{"props":{"allow_mult":"false"}}'
riak-admin bucket-type activate ycsb
```

* <i>strong consistency model</i>, type:

```
riak-admin bucket-type create ycsb '{"props":{"allow_mult":"false","consistent":true}}'
riak-admin bucket-type activate ycsb
```
Note that you may want to specify the number of replicas to create for each object. To do so, you can add `"n_val":N` to the list of properties shown above (by default `N` is set to 3).

Riak KV configuration parameters
----------------------------
You can either specify these configuration parameters via command line or set them in the `riak.properties` file.

* `riak.hosts` - <b>string list</b>, comma separated list of IPs or FQDNs. <newline>Example: `riak.hosts=127.0.0.1,127.0.0.2,127.0.0.3` or `riak.hosts=riak1.mydomain.com,riak2.mydomain.com,riak3.mydomain.com`.
* `riak.port` - <b>int</b>, the port on which every node is listening. It must match the one specified in the `riak.conf` file at the line `listener.protobuf.internal`.
* `riak.bucket_type` - <b>string</b>, it must match the value of the bucket type created during setup (see section above).
* `riak.r_val` - <b>int</b>, the R value represents the number of Riak nodes that must return results for a read before the read is considered successful.
* `riak.w_val` - <b>int</b>, the W value represents the number of Riak nodes that must report success before an update is considered complete.
* `riak.read_retry_count` - <b>int</b>, the number of times the client will try to read a key from Riak.
* `riak.wait_time_before_retry` - <b>int</b>, the time (in milliseconds) before client attempts to perform another read if the previous one failed.
* `riak.transaction_time_limit` - <b>int</b>, the time (in seconds) the client waits before aborting the current transaction.
* `riak.strong_consistency` - <b>boolean</b>, indicates whether to use strong consistency (true) or eventual consistency (false).
* `riak.debug` - <b>boolean</b>, enables debug mode. This displays all the properties (specified or defaults) when a benchmark is started. Moreover, it shows error causes whenever these occur.

<b>Note</b>: For more information on workloads and how to run them please see: https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload

<b id="f1">1</b> As specified in the `riak.properties` file.  See parameters configuration section for further info. [↩](#a1)
