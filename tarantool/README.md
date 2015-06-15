# Tarantool

## Introduction

Tarantool is a NoSQL In-Memory database.
It's distributed under BSD licence and is hosted on [github][tarantool-github].

Tarantool features:

* Defferent index types with iterators:
	- HASH (the fastest)
	- TREE (range and ordered retreival)
	- BITSET (bit mask search)
	- RTREE (geo search)
* multipart keys for HASH and TREE indexes
* Data persistence with by Write Ahead Log (WAL) and snapshots.
* asynchronous master-master replication, hot standby.
* coroutines and async. IO are used to implement high-performance lock-free access to data.
  - socket-io/file-io with yeilds from lua
* stored procedures in Lua (Using LuaJIT)
* supports plugins written on C/C++ (Have two basic plugins for working with MySQL and PostgreSQL)
* Authentication and access control

## Quick start

This section descrives how to run YCSB against a local Tarantool instance

### 1. Start Tarantool

First, clone Tarantool from it's own git repo and build it (described in our [README.md][tarantool-readme]):

    cp %YCSB%/tarantool/conf/tarantool-tree.lua <vardir>/tarantool.lua
    cp %TNT%/src/box/tarantool <vardir>
    cd <vardir>
    ./tarantool tarantool.lua

OR you can simply download ans install a binary package for your GNU/Linux or BSD distro from http://tarantool.org/download.html

### 2. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load tarantool -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run tarantool -s -P workloads/workloada

See the next section for the list of configuration parameters for Tarantool.

## Tarantool Configuration Parameters

#### 'tarantool.host' (default : 'localhost')
Which host YCSB must use for connection with Tarantool
#### 'tarantool.port' (default : 3301)
Which port YCSB must use for connection with Tarantool
#### 'tarantool.space' (default : 1024)
    (possible values: 0 .. 255)
Which space YCSB must use for benchmark Tarantool

[tarantool-github]: https://github.com/tarantool/tarantool/
[tarantool-readme]: https://github.com/tarantool/tarantool/blob/master/README.md
