<!--
Copyright (c) 2015 - 2021 YCSB contributors. All rights reserved.

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

# PmemKV Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a [PmemKV](https://github.com/pmem/pmemkv).
It uses the PmemKV Java bindings.

## Quick Start

### 0. Install PmemKV Java Binding
**Optionally** you can compile and install custom version of PmemKV Java binding.
The min. supported version is `1.2.0`.

>Note: If you want to use custom installation, you'll have to set additional
>maven parameter for building/execution of **PmemKV module**: `-Dpmemkv.packageName=pmemkv`.

Simple follow [PmemKV Java installation instruction](https://github.com/pmem/pmemkv-java#installation),
including at least:

    export JAVA_HOME=#PATH_TO_YOUR_JAVA_HOME
    git clone https://github.com/pmem/pmemkv-java.git
    cd pmemkv-java
    mvn install

### 1. Set Up YCSB
You need to clone the repository and compile **PmemKV module**.

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:pmemkv-binding -am package

Optionally, you can use specific pmemkv version, by adding extra maven parameter: `-Dpmemkv.packageVersion=X.Y.Z`

### 2. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

    bin/ycsb.sh load pmemkv -P workloads/workloada -p pmemkv.engine=cmap -p pmemkv.dbpath=/path/to/pmem/pool -p pmemkv.dbsize=DB_SIZE

Then, you can run the workload:

    bin/ycsb.sh run pmemkv -P workloads/workloada -p pmemkv.engine=cmap -p pmemkv.dbpath=/path/to/pmem/pool -p pmemkv.dbsize=DB_SIZE

## Configuration Options
Driver has several configuration options to parametrize engine, path, size and additional paramaters:

| Parameter             | Meaning                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| pmemkv.engine         | pmemkv's storage engine - name of one of the supported engines             |
| pmemkv.dbpath         | Pool file path (to run workloads in)                                       |
| pmemkv.dbsize         | Pool file size (required to create database file, if it doesn't exist yet) |
| pmemkv.jsonconfigfile | Extra config parameters                                                    |

The default engine used in YCSB (if not defined otherwise) is **cmap**.

To check possible values for storage engine see
[pmemkv's documentation](https://github.com/pmem/pmemkv#storage-engines).
Note that each engine may require different path (a file or a directory)
and support various min. size - it's described in the mentioned documentation.

While most of engines require just path and size, it is possible to specify
additional config parameters. You have to pass these extra config parameters
within a file with a proper JSON Object. It is then passed to the PmemKV module
as a path to this file (e.g. `pmemkv.jsonconfigfile=/path/to/json/file`).

>Example of a proper JSON Object: `{"param1": "abc", "param2": 123}`
