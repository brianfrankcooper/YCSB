<!--
Copyright (c) 2014 - 2021 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on RonDB. 

### 1. Start RonDB

### 2. Create Table

Create the following table in a database. Default DB name is ycsb which you can override with 
`rondb.schema` property. For the fields this benchmark only supports varbinary and varchar 
data types. Note that using varchar each charater takes 4 bytes using the default Utf8_unicode_ci 
encoding.

```sql
# one 4KB data column
CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 varbinary(4096))

OR
# ten data columns
CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 varchar(100), FIELD1 varchar(100), FIELD2 varchar(100), FIELD3 varchar(100), FIELD4 varchar(100), FIELD5 varchar(100), FIELD6 varchar(100), FIELD7 varchar(100), FIELD8 varchar(100), FIELD9 varchar(100));
```

*Note:* The number of columns must be equal to `fieldcount`, and the columns' length must not be less than `fieldlength`

### 3. Install Java and Maven

Install Java and Maven on your platform to build the benchmark


### 4. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/logicalclocks/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:rondb-binding -am clean package

### 5. Provide RonDB Connection Parameters
    
Set connection string, schema name, and fieldcount in the workload you plan to run. 

- `rondb.connection.string`  Default : 127.0.0.1:1186
- `rondb.schema`  Default : ycsb 

#### Note
Set the ycsb `fieldcount` according to `usertable` schema. For example, in the case of above table set `fieldcount=1`

You can also set configs with the shell command, e.g.:

    ./bin/ycsb load rondb -s -P workloads/workloada -p "rondb.connection.string=127.0.0.1:1186" -p "rondb.schema=ycsb" -p "fieldcount=1" > outputLoad.txt

### 6. Load data and run tests

Make sure that the RonDB native client library `libndbclient.so` is included in the `LD_LIBRARY_PATH`

Load the data:

    ./bin/ycsb load rondb -s -P workloads/workloada -p "rondb.connection.string=127.0.0.1:1186" -p "rondb.schema=ycsb" -p "fieldcount=1"  -p "fieldlength=4096"  -p "fieldnameprefix=FIELD" 

Run the workload test:

    ./bin/ycsb run rondb -s -P workloads/workloada -p "rondb.connection.string=127.0.0.1:1186" -p "rondb.schema=ycsb" -p "fieldcount=1"  -p "fieldlength=4096" -p "fieldnameprefix=FIELD" 

