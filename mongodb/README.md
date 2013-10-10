## Quick Start

This section describes how to run YCSB on MongoDB running locally. 

### 1. Start MongoDB

First, download MongoDB and start `mongod`. For example, to start MongoDB
on x86-64 Linux box:

    wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-x.x.x.tgz
    tar xfvz mongodb-linux-x86_64-*.tgz
    mkdir /tmp/mongodb
    cd mongodb-linux-x86_64-*
    ./bin/mongod --dbpath /tmp/mongodb

### 2. Install Maven and Java

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

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 4. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load mongodb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run mongodb -s -P workloads/workloada

See the next section for the list of configuration parameters for MongoDB.

## MongoDB Configuration Parameters

- `mongodb.url` default: `mongodb://localhost:27017`

- `mongodb.database` default: `ycsb`

- `mongodb.writeConcern` default `acknowledged`
 - options are :
  - `errors_ignored`
  - `unacknowledged`
  - `acknowledged`
  - `journaled`
  - `replica_acknowledged`

- `mongodb.readPreference` default `primary`
 - options are :
  - `primary`
  - `primary_preferred`
  - `secondary`
  - `secondary_preferred`
  - `nearest`

- `mongodb.maxconnections` (default `100`)

- `mongodb.threadsAllowedToBlockForConnectionMultiplier` (default `5`)

For example:

    ./bin/ycsb load mongodb -s -P workloads/workloada -p mongodb.writeConcern=unacknowledged

