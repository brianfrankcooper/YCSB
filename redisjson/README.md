<!--
Copyright (c) 2014 - 2020 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on RedisJson. 

### 1. Start Redis with the RedisJSON module loaded (cf. https://oss.redislabs.com/redisjson/#building-and-loading-the-module)

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:redisjson-binding -am clean package
	
### 4. Set up JRedisJSON
	In a separate directory ( the JAR will be installed in your local
	maven repo ):
	git clone https://github.com/RedisJSON/JRedisJSON.git 
	( You need at least 1.2.0, otherwise the client side
	won't be able to instantiate the right class when retrieving an
	object from the DB using JSON.GET )
	mvn clean install -Dmaven.test.skip=true
	( Make sure that you have a Redis instance running if you omit the
	-Dmaven.test.skip=true )

### 5. Provide Redis Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `redis.host`
- `redis.port`
- `redis.password`
  * Don't set the password if redis auth is disabled.
- A note on clusters: the current implementation of the redisjson
  interface code does not support  clustered instances yet - feel free
  to submit a PR :-)

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load redisjson -s -P workloads/workloada -p "redis.host=127.0.0.1" -p "redis.port=6379" > outputLoad.txt

### 6. Load data and run tests

Load the data:

    ./bin/ycsb load redisjson -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redisjson -s -P workloads/workloada > outputRun.txt
