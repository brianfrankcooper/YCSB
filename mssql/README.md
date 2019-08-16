<!--
Copyright (c) 2019 YCSB contributors. All rights reserved.

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

<!-- Hi! I'm KowsarAtz, Email: kowsar.atazadeh@gmail.com -->

# MSSQL BINDING FOR YCSB
This module is a modification of jdbc binding that enables YCSB to work with mssql databases.
If you need to upgrade current dependencies or add new ones, modify pom.xml in mssql directory and repeat the step 2.

## Getting Started

### 1. Start your database
Start your SQL Server Database service.

### 2. Set up MSSQL BINDING
```
mvn -pl com.yahoo.ycsb:mssql-binding -am clean package -Dcheckstyle.skip -DskipTests
```
### 3. Configure your database and table.
```
CREATE DATABASE ycsb;
go
use ycsb;
go
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT
);
go
```
### 4. Configure YCSB connection properties (EXP: db.properties file in YCSB directory)
```
db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
db.url=jdbc:sqlserver://<server>:<port>;databaseName=ycsb
db.user=<username>
db.passwd=<password>
```

### 5. Loading and Running Workloads
#### load:
```
./bin/ycsb.sh load mssql -P workloads/workloade -P mssql/db.properties
```
#### run:
```
./bin/ycsb.sh run mssql -P workloads/workloade -P mssql/db.properties
```
