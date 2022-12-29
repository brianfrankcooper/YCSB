<!--
Copyright (c) 2014 - 2021 YCSB contributors. All rights reserved.
Copyright (c) 2022 Hopsworks AB contributors. All rights reserved.

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

# Quick Start

This section describes how to run YCSB on RonDB.

1. Start RonDB

2. Create Table

    Create the following table in a database. YCSB will assume the database name is "ycsb" by default. This can be changed in the workload configuration file, as described below. For the fields this benchmark only supports varbinary and varchar data types. Note that using varchar each character takes 4 bytes using the default Utf8_unicode_ci encoding.

    ```sql
    -- one 4KB data column
    CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 varbinary(4096))

    -- OR

    -- ten 400B data columns
    CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 varchar(100), FIELD1 varchar(100), FIELD2 varchar(100), FIELD3 varchar(100), FIELD4 varchar(100), FIELD5 varchar(100), FIELD6 varchar(100), FIELD7 varchar(100), FIELD8 varchar(100), FIELD9 varchar(100));
    ```

    *Note:* The number of columns must be equal to `fieldcount`, and the columns' length must not be less than `fieldlength`

3. Install Java and Maven

    Install Java and Maven on your platform to build the benchmark

4. Set Up YCSB

    Git clone YCSB and compile:
    ```bash
    git clone http://github.com/logicalclocks/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:rondb-binding -am clean package
    ```

5. Make sure that the RonDB native client library `libndbclient.so` is included in the `LD_LIBRARY_PATH` for this.

6. Customise workload configuration

    Specify the desired benchmark configurations using a custom or pre-defined [workload file](../workloads/).

    Inside the workload file, define RonDB-specific parameters:
    - `rondb.connection.string`  Default: 127.0.0.1:1186
    - `rondb.schema`  I.e. database name; Default: ycsb
    - `rondb.use.rest.api` Default: false; this means it will use ClusterJ; Rest API specific:
        - `rondb.rest.api.batch.size` Default: 1
        - `rondb.rest.server.ip` Default: localhost
        - `rondb.rest.server.port` Default: 5000
        - `rondb.rest.api.version` Default: 0.1.0

    Also, set the `fieldcount`, `fieldlength` and `fieldnameprefix` according to `usertable` schema. From the SQL examples above, this would be one of the two:
    - "fieldcount=1", "fieldlength=4096", "fieldnameprefix=FIELD"
    - "fieldcount=10", "fieldlength=400", "fieldnameprefix=FIELD" (4 bytes per varchar character using Utf8_unicode_ci encoding)

7. Load the data

    ```bash
    # Use -p flag to overwrite any parameters in the specified workload file
    ./bin/ycsb load rondb -s -P workloads/workloada \
        -p "rondb.connection.string=127.0.0.1:1186" \
        -p "rondb.schema=ycsb" \
        -p "rondb.use.rest.api=true" \
        -p "fieldcount=1"  \
        -p "fieldlength=4096"  \
        -p "fieldnameprefix=FIELD"
    ```

8. Run the workload test

    ```bash
    # Use -p flag to overwrite any parameters in the specified workload file
    ./bin/ycsb run rondb -s -P workloads/workloada \
        -p "rondb.connection.string=127.0.0.1:1186" \
        -p "rondb.schema=ycsb" \
        -p "rondb.use.rest.api=true" \
        -p "fieldcount=1"  \
        -p "fieldlength=4096" \
        -p "fieldnameprefix=FIELD" 
    ```
