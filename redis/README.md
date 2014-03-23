## Quick Start

This section describes how to run YCSB on MongoDB. 

### 1. Start Redis

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:core,com.yahoo.ycsb:redis-binding clean package

### 4. Provide Redis Connection Parameters
    
Set the host, port, and password (do not redis auth is not turned on) in the 
workload you plan to run.

- `redis.url`
- `redis.port`
- `redis.password`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load redis -s -P workloads/workloada -p "redis.host=127.0.0.1" -p "redis.port=6379" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load redis -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redis -s -P workloads/workloada > outputRun.txt
    
