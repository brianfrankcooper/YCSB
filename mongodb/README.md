## Quick Start

This section describes how to run YCSB on MongoDB running locally. 

### 1. Start MongoDB

First, download MongoDB and start `mongod`. For example, to start MongoDB
on x86-64 Linux box:

    wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-1.8.3.tgz
    tar xfvz mongodb-linux-x86_64-1.8.3.tgz
    mkdir /tmp/mongodb
    cd mongodb-linux-x86_64-1.8.3
    ./bin/mongod --dbpath /tmp/mongodb

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone [put YCSB git repository URL]
    cd YCSB
    mvn clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load mongodb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run mongodb -s -P workloads/workloada

See the next section for the list of configuration parameters for MongoDB.

## MongoDB Configuration Parameters

### `mongodb.url` (default: `mongodb://localhost:27017`)

### `mongodb.database` (default: `ycsb`)

### `mongodb.writeConcern` (default `safe`)

### `mongodb.readPreference` (default `primary`)

### `mongodb.maxconnections` (default `10`)
