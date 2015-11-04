<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2015 YCSB contributors.
All rights reserved.

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

# Install Hypertable

Installation instructions for Hypertable can be found at:

    code.google.com/p/hypertable/wiki/HypertableManual


# Set Up YCSB

Clone the YCSB git repository and compile:

    ]$ git clone git://github.com/brianfrankcooper/YCSB.git
    ]$ cd YCSB
    ]$ mvn clean package

# Run Hypertable

Once it has been installed, start Hypertable by running

    ]$ ./bin/ht start all-servers hadoop

if an instance of HDFS is running or

    ]$ ./bin/ht start all-servers local

if the database is backed by the local file system. YCSB accesses
a table called 'usertable' by default. Create this table through the
Hypertable shell by running

    ]$ ./bin/ht shell
    hypertable> use '/ycsb';
    hypertable> create table usertable(family);
    hypertable> quit

All iteractions by YCSB take place under the Hypertable namespace '/ycsb'.
Hypertable also uses an additional data grouping structure called a column 
family that must be set. YCSB doesn't offer fine grained operations on 
column families so in this example the table is created with a single 
column family named 'family' to which all column families will belong. 
The name of this column family must be passed to YCSB. The table can be 
manipulated from within the hypertable shell without interfering with the
operation of YCSB. 

# Run YCSB

Make sure that an instance of Hypertable is running. To access the database
through the YCSB shell, from the YCSB directory run:

    ]$ ./bin/ycsb shell hypertable -p columnfamily=family

where the value passed to columnfamily matches that used in the table
creation. To run a workload, first load the data:

    ]$ ./bin/ycsb load hypertable -P workloads/workloada -p columnfamily=family

Then run the workload:

    ]$ ./bin/ycsb run hypertable -P workloads/workloada -p columnfamily=family

This example runs the core workload 'workloada' that comes packaged with YCSB.
The state of the YCSB data in the Hypertable database can be reset by dropping
usertable and recreating it.

# Configuration Parameters

Hypertable configuration settings can be found in conf/hypertable.cfg under 
your main hypertable directory. Make sure that the constant THRIFTBROKER_PORT
in the class HypertableClient matches the setting ThriftBroker.Port in 
hypertable.cfg.

To change the amount of data returned on each call to the ThriftClient on
a Hypertable scan, one must add a new parameter to hypertable.cfg. Include
ThriftBroker.NextThreshold=x where x is set to the size desired in bytes.
The default setting of this parameter is 128000.

To alter the Hypertable namespace YCSB operates under, change the constant
NAMESPACE in the class HypertableClient.
