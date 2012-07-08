## Quick Start

This section describes how to run YCSB on Voldemort running locally. 

### 1. Download Voldemort and YCSB

    wget https://github.com/downloads/voldemort/voldemort/voldemort-0.90.1.tar.gz
    tar xfvz voldemort-0.90.1.tar.gz

    git clone git://github.com/brianfrankcooper/YCSB.git

### 2. Setup and run Voldemort

We have to copy the sample YCSB config for Voldemort before we can run the server:

    cd voldemort-0.90.1
    mkdir -p config/ycsb/config/
    cp -r ../YCSB/voldemort/src/main/conf/* config/ycsb/config/
    ./bin/voldemort-server.sh config/ycsb

### 3. Set Up YCSB

Install voldermort into your local maven repository:

    cd voldemort-0.90.1/dist/
    mvn install:install-file -DgroupId=voldemort -DartifactId=voldemort -Dversion=0.90.1 -Dpackaging=jar -Dfile=voldemort-0.90.1.jar

Compile YCSB:

    cd YCSB
    mvn clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load voldemort -s -P workloads/workloada -p bootstrap_urls=tcp://localhost:6666

Then, run the workload:

    ./bin/ycsb run  voldemort -s -P workloads/workloada -p bootstrap_urls=tcp://localhost:6666


## Some Voldemort Configuration Parameters

* `boostrap_url` (required)
* `store_name` (default: `usertable`, same as `-t <table>`)

