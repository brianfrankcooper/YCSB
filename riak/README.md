Riak Client for Yahoo! Cloud System Benchmark (YCSB)
--------------------------------------------------------

The Riak YCSB client is designed to work with the Yahoo! Cloud System Benchmark (YCSB) project (https://github.com/brianfrankcooper/YCSB) to support performance testing for the 2.0.X line of the Riak database.

How to Implement the Riak Client
----------------------------
The following directions will help you get started with benchmarking Riak using the YCCB project and Riak client.

* Download the YCSB project from https://github.com/brianfrankcooper/YCSB and extract the contents onto the machine, or machines, you plan to execute the project from. <b>Note</b>: YCSB requires Java and Maven.

* Download the YCSB-Riak-Binding project and copy the Riak folder into the YCSB folder.

* Modify the following sections of the YCSB's POM file to add the Riak client:

```
<properties>
  ...
  <riak.version>2.0.2</riak.version>
  ...
</properties>
```

```
<modules>
  ...
  <module>riak</module>
  ...
</modules>
```

* Modify the <b>ycsb</b> file in the YCSB <b>bin</b> folder to add the Riak client in the DATABASES section as shown below::
```
DATABASES = {
    "basic"        : "com.yahoo.ycsb.BasicDB",
    ...
    "riak"         : "com.yahoo.ycsb.db.RiakDBClient"
}
```

* Perform the following operations on your Riak cluster to configure Riak for the benchmarks:

<blockquote>
Set the default backend for Riak to LevelDB in riak.conf (required to support secondary indexes used for the "scan" workloads)*:
</blockquote>

```
storage_backend = leveldb
```

<blockquote>
Create the "ycsb" bucket type by logging into one of the nodes in your cluster and run the following riak-admin commands:
</blockquote>

```
riak-admin bucket-type create ycsb '{"props":{"allow_mult":"false"}}'
riak-admin bucket-type activate ycsb
```  

* Modify NODES_ARRAY in RiakDBClient.java to include all of the nodes in your Riak test cluster.

```
// Array of nodes in the Riak cluster or load balancer in front of the cluster
private static final String[] NODES_ARRAY = {"127.0.0.1"};
```

* Build the YCSB project by running the following command in the YCSB root

```
mvn clean package
```

* Load and run a YCSB workload using the Riak client:

```
./bin/ycsb load riak -P workloads/workloada
./bin/ycsb run riak -P workloads/workloada
```

<b>Note</b>: For more information on workloads and how to run them please see: https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
