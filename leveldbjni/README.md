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

## Quick Start - The way to connect ycsb and your own leveldb

This section describes how to run YCSB on Redis. 

### 1. Code JNI for leveldb and get a leveldb-jni.jar

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:redis-binding -am clean package

### 4. Set the dependcy about leveldb-jni.jar in pom.xml
    
 <dependency>
      <groupId>org.fusesource.leveldbjni</groupId>
      <artifactId>leveldbjni-all</artifactId>
      <version>1.7</version>
      <scope>system</scope>
      <systemPath>${project.basedir}/lib/leveldbjni-all-1.7.jar</systemPath>
 </dependency>

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load redis -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redis -s -P workloads/workloada > outputRun.txt
