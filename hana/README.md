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

## Installing SAP Hana

Hana is provided and maintained by SAP. A full manual and tutorial for all it's components can be found
at [their official webpage][sap-tutorial-master].

In the following, this assumes you do have an installation medium from SAP. 
Additionally this guide assumes you'll be installing only on a single host.
Should that not be the case, the official guide to installing a "Multiple-Host SAP HANA System" is [here][sap-install-multi]

This guide is based on the [official SAP installation guide][sap-install-single]

 [sap-tutorial-master]: https://www.sap.com/products/hana/implementation/resources.html
 [sap-install-multi]: https://help.sap.com/viewer/2c1988d620e04368aa4103bf26f17727/2.0.02/en-US/5c5163079e264b8a951735fd1a63ab55.html
 [sap-install-single]: https://help.sap.com/viewer/2c1988d620e04368aa4103bf26f17727/2.0.02/en-US/a30b500d08d44a42b45b47105181605f.html
 
###Steps:

**Preparations**:

 1. Make sure you are logged in as `root`
 2. Make sure that the installation medium is accessible to all users
 3. Make sure that preexisting SAP HANA users are correctly configured.
 
**Installation**:

 1. Depending on the hardware platform you are using, enter one of the following folders:
 
        | Hardware Platform    | Folder       
        | Intel-Based Platform | <installation medium>/DATA_UNITS/HDB_LCM_LINUX_X86_64
        | IBM Power Systems    | <installation medium>/DATA_UNITS/HDB_LCM_LINUX_PPC64
     
 2. Start the Database Lifecycle Manager in the command line:
  
        ./hdblcm
        
 3. Select "Install New System".
 4. Select the components you want to install. This is *at least* the HANA Database.
 5. Specify the SAP HANA system properties.
 6. Review and confirm the summary with **y**.
 
## YCSB Integration

The binding requires the following properties to be set:

- `db.driver` **database driver**:
  The driver classname to load as JDBC driver.
  Note that the driver class **must** be available on the classpath.
  Furthermore the driver name **must** be a valid argument to java's `Class.forName` mechanism to load classes.

- `db.url` **database url**:
  A URL the database is reachable under. 
 
- `db.user` **database user**:
  A user with sufficient permissions to run the tests.
 
- `db.passwd` **user authentication password**:
  The password of the user specified to authenticate against the database.
  If not specified, this defaults to `""`.
 
 Additional tuning options include:
 
 - `jdbc.fetchsize`: 
  The fetchsize parameter of the JDBC connection to the database. 
  Will not be explicitly set by the adapter if not specified.
  
 - `jdbc.autocommit`:
  Whether to automatically commit transactions in the JDBC connection.
  Defaults to `true`.
  
 - `dogroupby`: 
  Group results in `SCAN` statements. Defaults to `true`.
  
 - `debug` **enable debug logging**:
  Verbose debug information is logged by the ycsb-binding.

 - `test` **test run**:
  If set to true, the ycsb-binding will build (and possibly log) queries but not execute them.

## YCSB Runs

The binding supports only timestamp workloads.
If non-timestamp-workloads are used, the binding is highly likely to not work.

```bash
bin/ycsb load hana -P workload/tsworkloada -P hana.properties
bin/ycsb run hana -P workload/tsworkloada -P hana.properties
```
