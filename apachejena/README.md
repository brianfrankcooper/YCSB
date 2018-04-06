<!--
Copyright (c) 2018 YCSB contributors. All rights reserved.

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

# Apache Jena TDB

This is a quick introduction for using the apachejena-binding.


Setup
-
Run the following command to build the apachejena-binding module:

    mvn -pl com.yahoo.ycsb:apachejena-binding -am clean package
    

Configuration
-
You can specify the location where the TDB should store its data with this parameter:

    -p outputdirectory=[path/to/database] (default=pathToYCSB/apachejena_database)


Example
-
Run a benchmark with the apachejena-binding.
Use the .sh or .bat if you are on mac/Linux or Windows respectively.
Example shown for a Linux system.

Loading data into the database:


    bin/ycsb.sh load apachejena -P workloads/graphworkloada -p outputdirectory=~/Desktop/test/

Running transactions on the database:

    bin/ycsb.sh run apachejena -P workloads/graphworkloada -p outputdirectory=~/Desktop/test/