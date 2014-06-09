Yahoo! Cloud System Benchmark (YCSB)
====================================

A note on comparing multiple systems
------------------------------------

NoSQL systems have widely varying defaults for trading off write durability vs performance.  Make sure that you are [comparing apples to apples across all candidates](http://www.datastax.com/dev/blog/how-not-to-benchmark-cassandra-a-case-study).  The most useful common denominator is synchronously durable writes.  The following YCSB clients have been verified to perform synchronously durable writes by default:

- Couchbase
- HBase
- MongoDB

Cassandra requires a configuration change in conf/cassandra.yaml.  Uncomment these lines:

    # commitlog_sync: batch
    # commitlog_sync_batch_window_in_ms: 50

Links
-----
http://wiki.github.com/jbellis/YCSB/  
http://research.yahoo.com/Web_Information_Management/YCSB/  
ycsb-users@yahoogroups.com  

Getting Started
---------------

1. Download the latest release of YCSB:

    ```sh
    wget https://github.com/downloads/jbellis/YCSB/ycsb-0.1.4.tar.gz
    tar xfvz ycsb-0.1.4
    cd ycsb-0.1.4
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 
    
    ```sh
    bin/ycsb load basic -P workloads/workloada
    bin/ycsb run basic -P workloads/workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

  Alternatively, see fabric/README for Thumbtack's work on parallelizing
  YCSB clients using Fabric.
