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

## DB creation with the OrientDBClient
This client will create a database for you if the connection database you specify does not exists. You can also specify connection information to a preexisting database.

You can use the ```orientdb.newdb=true``` property to allow this client to drop and create a new database instance during the ```load``` phase.

NOTE: understand that using the ```orientdb.newdb=true``` property will drop and recreate databases even if it was a preexisting instance.

WARNING: Creating a new database will be done safely with multiple threads on a single YCSB instance, but is not guaranteed to work when launching multiple YCSB instances. In that scenario it is suggested that you create the db before hand, or run the ```load``` phase with a single YCSB instance.

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
* ```orientdb.remote.storagetype``` - Storage type of the database on remote server
    * This is only required if using a ```remote:``` connection url

## Known Issues

* There is a performance issue around the scan operation. This binding uses OIndex.iterateEntriesMajor() which will return unnecessarily large iterators. This has a performance impact as the recordcount goes up. There are ideas in the works to fix it, track it here: [#568](https://github.com/brianfrankcooper/YCSB/issues/568).
* Iterator methods needed to perform scans are Unsupported in the OrientDB API for remote database connections and so will return NOT_IMPLEMENTED status if attempted.
