## Quick Start

This section describes how to run YCSB on Aerospike. 

### 1. Start Aerospike

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:aerospike-binding -am clean package

### 4. Provide Aerospike Connection Parameters

The following connection parameters are available.

  * `as.host` - The Aerospike cluster to connect to (default: `localhost`)
  * `as.port` - The port to connect to (default: `3000`)
  * `as.user` - The user to connect as (no default)
  * `as.password` - The password for the user (no default)
  * `as.timeout` - The transaction and connection timeout (in ms, default: `10000`)
  * `as.namespace` - The namespace to be used for the benchmark (default: `ycsb`)

Add them to the workload or set them with the shell command, as in:

    ./bin/ycsb load aerospike -s -P workloads/workloada -p as.timeout=5000 >outputLoad.txt

### 5. Load Data and Run Tests

Load the data:

    ./bin/ycsb load aerospike -s -P workloads/workloada >outputLoad.txt

Run the workload test:

    ./bin/ycsb run aerospike -s -P workloads/workloada >outputRun.txt

