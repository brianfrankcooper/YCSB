## Quick Start

This section describes how to run YCSB on OrientDB running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/nuvolabase/YCSB.git
    cd YCSB
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load orientdb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run orientdb -s -P workloads/workloada

See the next section for the list of configuration parameters for OrientDB.

## OrientDB Configuration Parameters

### `OrientDB.url` (default: `local:C:/temp/databases/ycsb`)

### `OrientDB.user` (default `admin`)

### `OrientDB.password` (default `admin`)
