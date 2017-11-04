<!--
Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.

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

# Angra-DB Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Angra-DB Server cluster.

## Quickstart

### 1. Start Angra-DB Server
You need to start an Angra-DB server. Please see [https://github.com/Angra-DB/core](https://github.com/Angra-DB/core)
for more details and instructions.

### 2. Set up YCSB
Just clone from master.

```
git clone https://github.com/Angra-DB/YCSB-Angra-DB.git
cd YCSB-Angra-DB
mvn clean package
```

If you need compile only Angra-DB module.
```
mvn -pl com.yahoo.ycsb:angra-binding -am clean package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load angra -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run angra -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property
(as seen in the next section) like this:

```
bin/ycsb run angra -s -P workloads/workloada -p angra.host=127.0.0.1
```

## Angra-DB Configuration Parameters

- `angra.host`
  - Default value is `127.0.0.1`

- `angra.port`
  - Default value is `1234`

- `angra.schema`

  - Default value of database is `ycsb`
