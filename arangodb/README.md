<!--
Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on ArangoDB. 

### 1. Start ArangoDB
See https://docs.arangodb.com/Installing/index.html

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

Clone this YCSB source code:

    git clone https://github.com/brianfrankcooper/YCSB.git

### 4. Run YCSB

Now you are ready to run! First, drop the existing collection: "usertable" under database "ycsb":
	
	db._collection("usertable").drop()

Then, load the data:

    ./bin/ycsb load arangodb -s -P workloads/workloada -p arangodb.ip=xxx -p arangodb.port=xxx

Then, run the workload:

    ./bin/ycsb run arangodb -s -P workloads/workloada -p arangodb.ip=xxx -p arangodb.port=xxx

See the next section for the list of configuration parameters for ArangoDB.

### 5. Run against ArangoDB 3.0 and previews versions

Running YCSB on ArangoDB in version 3.0 or previews versions requires to use HTTP as network protocol. Since VST (VelcoyStream) is the default used protocol one have to set the configuration parameter `arangodb.protocol` to `HTTP_JSON`. For more infos take a look into the official [ArangoDB Java Driver Docs](https://github.com/arangodb/arangodb-java-driver/blob/master/docs/Drivers/Java/Reference/README.md#network-protocol). 

## ArangoDB Configuration Parameters

- `arangodb.ip`
  - Default value is `localhost`

- `arangodb.port`
  - Default value is `8529`.

- `arangodb.protocol`
  - Default value is 'VST'

- `arangodb.waitForSync`
  - Default value is `true`.
  
- `arangodb.transactionUpdate`
  - Default value is `false`.

- `arangodb.dropDBBeforeRun`
  - Default value is `false`.
