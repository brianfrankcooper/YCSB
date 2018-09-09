<!--
Copyright (c) 2015,2018 YCSB contributors. All rights reserved.

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

# Crate Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a [Crate Server](https://crate.io/) cluster. It uses the official [Crate JDBC Driver](https://crate.io/docs/clients/jdbc/en/latest/) and provides a rich set of configuration options.

# Quickstart

## CrateDB Setup
To get started quickly, you can test out the binding against crate running in a single docker node:

```
docker pull crate
docker run -d -p 4200:4200 -p 5432:5432 crate crate
```

Browse to the admin UI on http://localhost:4200/ and follow the [Crate Reference Documentation](https://crate.io/docs/crate/reference/en/latest/) to set up our test table

* Create a table, e.g. 'usertable' with a primary key named 'ycsb_key' and fields that correspond to your chosen field name prefix (default 'field') and the number of fields you'll use (default '10'); e.g. 'field1', 'field2', etc. (see ['Creating Tables' in the Crate Admin guide](https://crate.io/docs/crate/reference/en/latest/general/ddl/create-table.html))
* Create a user, e.g. 'ycsb' and note the password (see ['User Management' in the Crate Admin guide](https://crate.io/docs/crate/reference/en/latest/admin/user-management.html))
* grant the user you created access to this table. You will need to make sure they have both the "DQL" and "DML" permissions.


## Run YCSB

Use the same configs as you would for the JDBC binding. See [Crate JDBC Driver docs on "how to connect" for details](https://crate.io/docs/clients/jdbc/en/latest/connect.html#database-connection-urls).

Important properties to get started:

* **db.url** should point at your CrateDB cluster. Given the example Docker node above, you'd use `jdbc:crate://localhost:5432/`
* **db.user** should be a user with read/write access to your table.
* **db.passwd** should be the password that corresponds to that user

```
./bin/ycsb load crate -P workloads/workloada -p db.url=jdbc:crate://localhost:5432/ -p db.user=ycsb -p db.passwd=password
./bin/ycsb run crate -P workloads/workloada -p db.url=jdbc:crate://localhost:5432/ -p db.user=ycsb -p db.passwd=password
```

