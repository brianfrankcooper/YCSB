## Quick start

This section descrives how to run YCSB on Tarantool running locally.

### 1. Start Tarantool

First, clone Tarantool from it's own git repo and build it(You'll need cmake >= 2.6 and gcc ~>= 4.4):

	git clone git://github.com/mailru/tarantool.git -b master-stable
	cd tarantool
	cmake .
	make
	cp test/box/tarantool.cfg test/var/
	cp src/box/tarantool_box test/var/
	cd test/var
	./tarantool_box

OR you can simply download ans install binary package for your GNU/Linux or BSD distro from http://tarantool.org/download.html

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load tarantool -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run tarantool -s -P workloads/workloada

See the next section for the list of configuration parameters for Tarantool.

## Tarantool Configuration Parameters

### 'tnt.host' (default : 'localhost')

### 'tnt.port' (default : 33013)
