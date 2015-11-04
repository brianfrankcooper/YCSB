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

# JDBC Driver for YCSB
This driver enables YCSB to work with databases accessible via the JDBC protocol.

## Getting Started
### 1. Start your database
This driver will connect to databases that use the JDBC protocol, please refer to your databases documentation on information on how to install, configure and start your system.

### 2. Set up YCSB
You can clone the YCSB project and compile it to stay up to date with the latest changes. Or you can just download the latest release and unpack it. Either way, instructions for doing so can be found here: https://github.com/brianfrankcooper/YCSB.

### 3. Configure your database and table.
You can name your database what ever you want, you will need to provide the database name in the JDBC connection string.

You can name your table whatever you like also, but it needs to be specified using the YCSB core properties, the default is to just use 'usertable' as the table name.

The expected table schema will look similar to the following, syntactical differences may exist with your specific database:

```sql
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT
);
```

Key take aways:

* The primary key field needs to be named YCSB_KEY
* The other fields need to be prefixed with FIELD and count up starting from 1
* Add the same number of FIELDs as you specify in the YCSB core properties, default is 10.
* The type of the fields is not so important as long as they can accept strings of the length that you specify in the YCSB core properties, default is 100.

#### JdbcDBCreateTable Utility
YCSB has a utility to help create your SQL table. NOTE: It does not support all databases flavors, if it does not work for you, you will have to create your table manually with the schema given above. An example usage of the utility:

```sh
java -cp YCSB_HOME/jdbc-binding/lib/jdbc-binding-0.4.0.jar:mysql-connector-java-5.1.37-bin.jar com.yahoo.ycsb.db.JdbcDBCreateTable -P testworkload -P db.properties -n usertable
```

Hint: you need to include your Driver jar in the classpath as well as specify your loading options via a workload file, JDBC connection information, and a table name with ```-n```. 

Simply executing the JdbcDBCreateTable class without any other parameters will print out usage information.

### 4. Configure YCSB connection properties
You need to set the following connection configurations:

```sh
db.driver=com.mysql.jdbc.Driver
db.url=jdbc:mysql://127.0.0.1:3306/ycsb
db.user=admin
db.passwd=admin
```

Be sure to use your driver class, a valid JDBC connection string, and credentials to your database.

You can add these to your workload configuration or a separate properties file and specify it with ```-P``` or you can add the properties individually to your ycsb command with ```-p```.

### 5. Add your JDBC Driver to the classpath
There are several ways to do this, but a couple easy methods are to put a copy of your Driver jar in ```YCSB_HOME/jdbc-binding/lib/``` or just specify the path to your Driver jar with ```-cp``` in your ycsb command.

### 6. Running a workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

Then, you can run the workload:

```sh
bin/ycsb run jdbc -P workloads/workloada -P db.properties -cp mysql-connector-java.jar
```

## Configuration Properties

```sh
db.driver=com.mysql.jdbc.Driver				# The JDBC driver class to use.
db.url=jdbc:mysql://127.0.0.1:3306/ycsb		# The Database connection URL.
db.user=admin								# User name for the connection.
db.passwd=admin								# Password for the connection.
jdbc.fetchsize=10							# The JDBC fetch size hinted to the driver.
jdbc.autocommit=true						# The JDBC connection auto-commit property for the driver.
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.
