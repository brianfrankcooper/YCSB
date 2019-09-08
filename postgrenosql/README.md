<!--
Copyright (c) 2017 YCSB contributors. All rights reserved.

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
This section describes how to run YCSB on PostgreSQL and use it as a document store.
 
## Download and Install Postgres Server Version
You can find an instruction here: https://www.postgresql.org/download/.
Otherwise download the official docker container from https://hub.docker.com/_/postgres/ and follow the instructions.
Do not forget to expose the postgresql server port when running the container.
The YCSB PostgreNoSQL is tested on PostgreSQL Server 9.5 and 9.6.
 
In the next step grant access rights for the default postgres user to access the Postgres Server.
In case you run the YCSB client and the Postgres Server on different machines make sure that the server is reachable.
Then create a database, e.g., test and create a table that is able to store the documents. 

The following commands can be used for that:

**CREATE DATABASE test;**

**CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY not NULL, YCSB_VALUE JSONB not NULL);**

**GRANT ALL PRIVILEGES ON DATABASE test to postgres;**

## Configure the parameters in the properties file
**postgrenosql.url = jdbc:postgresql://localhost:5432/test**

Defines the connection string to the Postgres Server. Replace localhost by the address of your Postgres Server, 5432 by the port your Server is listing to and test by the name of your database.

**postgrenosql.user = postgres**

Defines the username the client uses to connect to the Postgres Server.

**postgrenosql.passwd = postgres**

Defines the password of user the client uses to connect to the Postgres Server.

**postgrenosql.autocommit = true**

Defines whether transactions shoud by applied directly.

Here are some basic commands to start the benchmark from the root directory:

The following command loads the workload and uses the configuration defined in the properties file:
.\bin\ycsb load postgrenosql -P .\workloads\workloada -P .\postgrenosql\conf\postgrenosql.properties

The following command runs the workload and uses the configuration defined in the properties file:
.\bin\ycsb run postgrenosql -P .\workloads\workloada -P .\postgrenosql\conf\postgrenosql.properties


