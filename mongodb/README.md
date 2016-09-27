<!--
Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on MongoDB. 

### 1. Start MongoDB

First, download MongoDB and start `mongod`. For example, to start MongoDB
on x86-64 Linux box:

    wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-x.x.x.tgz
    tar xfvz mongodb-linux-x86_64-*.tgz
    mkdir /tmp/mongodb
    cd mongodb-linux-x86_64-*
    ./bin/mongod --dbpath /tmp/mongodb

Replace x.x.x above with the latest stable release version for MongoDB.
See http://docs.mongodb.org/manual/installation/ for installation steps for various operating systems.

### 2. Install Java and Maven

Go to http://www.oracle.com/technetwork/java/javase/downloads/index.html

and get the url to download the rpm into your server. For example:

    wget http://download.oracle.com/otn-pub/java/jdk/7u40-b43/jdk-7u40-linux-x64.rpm?AuthParam=11232426132 -o jdk-7u40-linux-x64.rpm
    rpm -Uvh jdk-7u40-linux-x64.rpm
    
Or install via yum/apt-get

    sudo yum install java-devel

Download MVN from http://maven.apache.org/download.cgi

    wget http://ftp.heanet.ie/mirrors/www.apache.org/dist/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
    sudo tar xzf apache-maven-*-bin.tar.gz -C /usr/local
    cd /usr/local
    sudo ln -s apache-maven-* maven
    sudo vi /etc/profile.d/maven.sh

Add the following to `maven.sh`

    export M2_HOME=/usr/local/maven
    export PATH=${M2_HOME}/bin:${PATH}

Reload bash and test mvn

    bash
    mvn -version

### 3. Set Up YCSB

Download the YCSB zip file and compile:

    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.5.0/ycsb-0.5.0.tar.gz
    tar xfvz ycsb-0.5.0.tar.gz
    cd ycsb-0.5.0

### 4. Run YCSB

Now you are ready to run! First, use the asynchronous driver to load the data:

    ./bin/ycsb load mongodb-async -s -P workloads/workloada > outputLoad.txt

Then, run the workload:

    ./bin/ycsb run mongodb-async -s -P workloads/workloada > outputRun.txt
    
Similarly, to use the synchronous driver from MongoDB Inc. we load the data: 

    ./bin/ycsb load mongodb -s -P workloads/workloada > outputLoad.txt

Then, run the workload:

    ./bin/ycsb run mongodb -s -P workloads/workloada > outputRun.txt
    
See the next section for the list of configuration parameters for MongoDB.

## Log Level Control
Due to the mongodb driver defaulting to a log level of DEBUG, a logback.xml file is included with this module that restricts the org.mongodb logging to WARN. You can control this by overriding the logback.xml and defining it in your ycsb command by adding this flag:

```
bin/ycsb run mongodb -jvm-args="-Dlogback.configurationFile=/path/to/logback.xml"
```

## MongoDB Configuration Parameters

- `mongodb.url`
  - This should be a MongoDB URI or connection string. 
    - See http://docs.mongodb.org/manual/reference/connection-string/ for the standard options.
    - For the complete set of options for the asynchronous driver see: 
      - http://www.allanbank.com/mongodb-async-driver/apidocs/index.html?com/allanbank/mongodb/MongoDbUri.html
    - For the complete set of options for the synchronous driver see:
      - http://api.mongodb.org/java/current/index.html?com/mongodb/MongoClientURI.html
  - Default value is `mongodb://localhost:27017/ycsb?w=1`
  - Default value of database is `ycsb`

- `mongodb.batchsize`
  - Useful for the insert workload as it will submit the inserts in batches inproving throughput.
  - Default value is `1`.

- `mongodb.upsert`
  - Determines if the insert operation performs an update with the upsert operation or a insert. 
    Upserts have the advantage that they will continue to work for a partially loaded data set.
  - Setting to `true` uses updates, `false` uses insert operations.
  - Default value is `false`.

- `mongodb.writeConcern`
  - **Deprecated** - Use the `w` and `journal` options on the MongoDB URI provided by the `mongodb.url`.
  - Allowed values are :
    - `errors_ignored`
    - `unacknowledged`
    - `acknowledged`
    - `journaled`
    - `replica_acknowledged`
    - `majority`
  - Default value is `acknowledged`.
 
- `mongodb.readPreference`
  - **Deprecated** - Use the `readPreference` options on the MongoDB URI provided by the `mongodb.url`.
  - Allowed values are :
    - `primary`
    - `primary_preferred`
    - `secondary`
    - `secondary_preferred`
    - `nearest`
  - Default value is `primary`.
 
- `mongodb.maxconnections`
  - **Deprecated** - Use the `maxPoolSize` options on the MongoDB URI provided by the `mongodb.url`.
  - Default value is `100`.

- `mongodb.threadsAllowedToBlockForConnectionMultiplier`
  - **Deprecated** - Use the `waitQueueMultiple` options on the MongoDB URI provided by the `mongodb.url`.
  - Default value is `5`.

For example:

    ./bin/ycsb load mongodb-async -s -P workloads/workloada -p mongodb.url=mongodb://localhost:27017/ycsb?w=0

To run with the synchronous driver from MongoDB Inc.:

    ./bin/ycsb load mongodb -s -P workloads/workloada -p mongodb.url=mongodb://localhost:27017/ycsb?w=0
