# Tarantool

## Introduction

Tarantool is a NoSQL In-Memory database.
It's distributed under BSD licence and is hosted on [github][tnt-github].

Tarantool features:

* Defferent index types: HASH (the fastest), TREE (range and ordered retreival), BITSET (bit  mask search).
* multipart keys for HASH and TREE indexes
* Data persistence with by Write Ahead Log (WAL) and snapshots.
* asynchronous replication, hot standby.
* coroutines and async. IO are used to implement high-performance lock-free access to data.
* stored procedures in Lua (Using LuaJIT)
* supports plugins written on C/C++ (Have two basic plugins for working with MySQL and PostgreSQL)
* supports memcached text protocol

## Quick start

This section descrives how to run YCSB against a local Tarantool instance

### 1. Start Tarantool

First, clone Tarantool from it's own git repo and build it (described in our [README.md][tnt-readme]):

	cp YCSB/tarantool/config/tarantool-tree.cfg <vardir>/tarantool.cfg
	cp TNT/src/box/tarantool_box <vardir>
	cd <vardir>
	./tarantool_box --init-storage
	./tarantool_box &

OR you can simply download ans install a binary package for your GNU/Linux or BSD distro from http://tarantool.org/download.html

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

#### 'tnt.host' (default : 'localhost')
Which host YCSB must use for connection with Tarantool
#### 'tnt.port' (default : 33013)
Which port YCSB must use for connection with Tarantool
#### 'tnt.space' (default : 0) 
    (possible values: 0 .. 255)
Which space YCSB must use for benchmark Tarantool
#### 'tnt.call' (default : false) 
    (possible values: false, true)
If tnt.call is set to True - you may benchmark Tarantool Lua bindings,
instead of Tarantool basic Protocol.

Tips: If you want to flush all data in space with number N use:
	
	echo 'lua box.space[N]:truncate()' | nc localhost 33015

[tnt-github]:https://github.com/tarantool/tarantool/
[tnt-readme]:https://github.com/tarantool/tarantool/blob/master/README.md
