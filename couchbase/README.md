# Couchbase Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Couchbase Server cluster. It uses the official Couchbase Java SDK and provides a rich set of configuration options.

## Quickstart

### 1. Start Couchbase Server
You need to start a single node or a cluster to point the client at. Please see [http://couchbase.com](couchbase.com) for more details and instructions.

### 2. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load couchbase -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run couchbase -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property (as seen in the next section) like this:

```
bin/ycsb run couchbase -s -P workloads/workloada -p couchbase.useJson=false
```

## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and implement the facilities on your own.

You can set the following properties (with the default settings applied):

 - couchbase.url=http://127.0.0.1:8091/pools => The connection URL from one server.
 - couchbase.bucket=default => The bucket name to use.
 - couchbase.password= => The password of the bucket.

 - couchbase.checkFutures=true => If the futures should be inspected (makes ops sync).
 - couchbase.persistTo=0 => Observe Persistence ("PersistTo" constraint).
 - couchbase.replicateTo=0 => Observe Replication ("ReplicateTo" constraint).
 - couchbase.json=true => Use json or java serialization as target format.

