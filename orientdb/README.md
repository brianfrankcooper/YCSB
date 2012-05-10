## Quick Start

This section describes how to run YCSB on OrientDB running locally. 

### 1. Start OrientDB

First, download OrientDB and start `mongod`. For example, to start OrientDB
on x86-64 Linux box:

    wget http://fastdl.OrientDB.org/linux/OrientDB-linux-x86_64-1.8.3.tgz
    tar xfvz OrientDB-linux-x86_64-1.8.3.tgz
    mkdir /tmp/OrientDB
    cd OrientDB-linux-x86_64-1.8.3
    ./bin/mongod --dbpath /tmp/OrientDB

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load OrientDB -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run OrientDB -s -P workloads/workloada

See the next section for the list of configuration parameters for OrientDB.

## OrientDB Configuration Parameters

### `OrientDB.url` (default: `OrientDB://localhost:27017`)

### `OrientDB.database` (default: `ycsb`)

### `OrientDB.writeConcern` (default `safe`)
