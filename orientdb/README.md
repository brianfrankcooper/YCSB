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

This section describes how to run YCSB on OrientDB running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load orientdb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run orientdb -s -P workloads/workloada

See the next section for the list of configuration parameters for OrientDB.

## OrientDB Configuration Parameters

* ```orientdb.url``` - (required) The address to your database.
    * Supported storage types: memory, plocal, remote
    * EX. ```plocal:/path/to/database```
* ```orientdb.user``` - The user to connect to the database with.
    * Default: ```admin```
* ```orientdb.password``` - The password to connect to the database with.
    * Default: ```admin```
* ```orientdb.newdb``` - Overwrite the database if it already exists.
    * Only effects the ```load``` phase.
    * Default: ```false```
* ```orientdb.intent``` - Declare an Intent to the database.
    * This is an optimization feature provided by OrientDB: http://orientdb.com/docs/2.1/Performance-Tuning.html#massive-insertion
    * Possible values are:
        * massiveinsert
        * massiveread
        * nocache
* ```orientdb.remote.storagetype``` - Storage type of the database on remote server
    * This is only required if using a ```remote:``` connection url

## Known Issues

* There is a performance issue around the scan operation. This binding uses OIndex.iterateEntriesMajor() which will return unnecessarily large iterators. This has a performance impact as the recordcount goes up. There are ideas in the works to fix it, track it here: [#568](https://github.com/brianfrankcooper/YCSB/issues/568).
* The OIndexCursor used to run the scan operation currently seems to be broken. Because of this, if the startkey and recordcount combination on a particular operation were to cause the iterator to go to the end, a NullPointerException is thrown. With sufficiently high record counts, this does not happen very often, but it could cause false negatives. Track that issue here: https://github.com/orientechnologies/orientdb/issues/5541.
* Iterator methods needed to perform scans are Unsupported in the OrientDB API for remote database connections and so will return NOT_IMPLEMENTED status if attempted.
