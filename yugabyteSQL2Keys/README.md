<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

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

# YugabyteSQL2Keys binding
This driver enables YCSB to work with Yugabyte DB using SQL. This binding which uses 2 column keys better models the workloadE with the first column identifying the thread and the second column identifying the post within the thread.

## Getting Started
### 1. Start your database
Start the database using steps mentioned here: https://docs.yugabyte.com/latest/deploy/manual-deployment/

### 2. Set up YCSB
Clone the YCSB project:

```
git clone https://github.com/yugabyte/YCSB.git && cd YCSB
```

Compile the code:
```
mvn clean package
```

We can also just compile the yugabyteSQL binding using:
```
mvn -pl yugabyteSQL2Keys -am clean package
```

### 3. Configure your database and table.
Create the Database and table using the ysqlsh tool.

```
bin/ysqlsh -h <ip> -c 'create database ycsb;'
bin/ysqlsh -h <ip> -d ycsb -c 'CREATE TABLE usertable (YCSB_KEY1 VARCHAR(255), YCSB_KEY2 VARCHAR(255), FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT, PRIMARY KEY (YCSB_KEY1, YCSB_KEY2));'
```

### 4. Configure YCSB connection properties
You need to set the following connection configurations in yugabyteSQL/db.properties:

```sh
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://<ip>:5433/ycsb;
db.user=postgres
db.passwd=
```

### 5. Running the workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load yugabyteSQL2Keys -P yugabyteSQL/db.properties -P workloads/workloada
```

Then, you can run the workload:

```sh
bin/ycsb run yugabyteSQL2Keys -P yugabyteSQL/db.properties -P workloads/workloada
```

## Other Configuration Properties

```sh
db.batchsize=1000             # The batch size for doing batched inserts. Defaults to 0. Set to >0 to use batching.
jdbc.fetchsize=10							# The JDBC fetch size hinted to the driver.
jdbc.autocommit=true						# The JDBC connection auto-commit property for the driver.
jdbc.batchupdateapi=false     # Use addBatch()/executeBatch() JDBC methods instead of executeUpdate() for writes (default: false)
db.batchsize=1000             # The number of rows to be batched before commit (or executeBatch() when jdbc.batchupdateapi=true)
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.
