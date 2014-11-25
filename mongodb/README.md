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

    wget https://github.com/achille/YCSB/archive/master.zip
    unzip master
    cd YCSB-master
    mvn -pl com.yahoo.ycsb:core,com.yahoo.ycsb:mongodb-binding clean package

### 4. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load mongodb -s -P workloads/workloada > outputLoad.txt

Then, run the workload:

    ./bin/ycsb run mongodb -s -P workloads/workloada > outputRun.txt

See the next section for the list of configuration parameters for MongoDB.

## MongoDB Configuration Parameters

- `mongodb.url` default: `mongodb://localhost:27017`

- `mongodb.database` default: `ycsb`

- `mongodb.writeConcern` default `journaled`

 - options are :
  - `errors_ignored`
  - `unacknowledged`
  - `acknowledged`
  - `journaled`
  - `replica_acknowledged`

For example:

    ./bin/ycsb load mongodb -s -P workloads/workloada -p mongodb.writeConcern=replica_acknowledged
