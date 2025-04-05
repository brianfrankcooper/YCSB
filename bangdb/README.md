<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on BangDB. 

### 1. Start BangDB

First, download BangDB and run following command to start the server
	./bangdb-server-2.0 -d ycsb

For example for ubuntu 18, x86-64 box 
	wget http://bangdb.com/downloads/bangdb_2.0_ubuntu18.tar.gz
	tar -xzvf bangdb_2.0_ubuntu18.tar.gz
	cd bangdb_2.0_ubuntu18
	bash install.sh
	cd bin
	./bangdb-server-2.0 -d ycsb -b yes
	
For other linux flavors (centos7, centos8, rhel8, ubuntu 16, ubuntu 20) take appropriate tar file
See https://bangdb.com/download for installation steps for different operating systems

### 2. Install Java and Maven

Install java8 and maven 3.x.x

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone https://github.com/brianfrankcooper/YCSB
    cd YCSB
    mvn -pl site.ycsb:bangdb-binding -am clean package

### 4. Provide BangDB Connection Parameters
    
copy the bangdb.config file at the base folder ( YCSB folder )	
You will find bangdb.config file in the bangdb_2.0_ubuntu18/bin folder
simply copy this file to YCSB folder
	cp <bangdb-install-folder>/bangdb_2.0_ubuntu18/bin/bangdb.config .

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load bangdb -threads 128 -s -P workloads/workloada > outputLoada.txt

Run the workload test:

    ./bin/ycsb run bangdb -threads 128 -s -P workloads/workloada

etc...

NOTE: 

Please see these points if you wish to improve the performance...

A. The default settings for workloads is for 1000 keys (and values), which is very less in order
   to figure out the right throughput. Hence it will be better to change (increase) the number to
   1M or 10M or more and then run the tests and comparisons.
   You can edit the file workloads/workloada and all other workloads as needed.

B. You may also alter the number of threads, if the machine is good enough (16GB RAM and 8 cores or more),
   then it will be good to run with 200 or more threads (see item D below, we should also have appropriate
   num for NUM_CONNECTIONS_IN_POOL for optimum performance). 
   If the machine has 16GB+ RAM then it will be good idea to change the page size of the db to 32KB from
   default 16KB. Change the param PAGE_SIZE_BANGDB in the bangdb.config file

C. If you plan to run for larger number of key and values, then you should allocate more memory to BangDB. 
   You may do this by going to the bangdb install folder, go to bin/ directory, edit the bangdb.config file,
   and increase the size of pool (BUFF_POOL_SIZE_HINT) and set to higher number, such as 12GB on a 16GB machine,
   or 27GB on 32 GB machine etc.. The default value is 4GB, which is not a high number to run tests for more then couple
   of millions of keys

D. Finally, you must also increase the number of concurrent connections that a client can open with the server. This
   can be done by editing the bangdb.config file present in the YCSB base folder. Open the file and edit the 
   variable NUM_CONNECTIONS_IN_POOL and assign it to 128 or 200 (this should be around nthreads argument above)

E. It's recommended to clean up the db and restart for each test so we have right numbers for all the tests. 
   run
      kill -int $(pidof bangdb-server-2.0)
   then clean the data by deleting it (go to /bangdb_2.0_ubuntu18/bin/ folder)
      rm -rf data logdir
   now restart the db
      ./bangdb-server-2.0 -d ycsb -b yes

