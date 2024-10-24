### 1. Set Up YCSB

Build  RocksDB

	make rocksdbjava -j 10 DEBUG_LEVEL=0
	cp ./java/target/rocksdbjni-8.3.2-linux64.jar ../YCSB/rocksdb/build/


Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:rocksdb-binding -am clean package

### 2. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data

Then, run the workload:

    ./bin/ycsb run rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data

