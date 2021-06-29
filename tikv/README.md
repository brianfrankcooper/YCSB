<!--
Copyright (c) 2021 YCSB contributors. All rights reserved.

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
# TiKV Client for YCSB
A Test for TiKV's Java Client

## How to build
First build the latest TiKV Java Client from [repo](https://github.com/tikv/client-java).

Then build the YCSB project with following commands.

```commandline
mvn -pl site.ycsb:tikv-binding -am clean package
```

## How to run

The generated ycsb tar is located in `/path-to-repo/tikv/target/ycsb-tikv-binding-0.18.0-SNAPSHOT.tar.gz`

The following command starts a ycsb workload.

```commandline
tar -xvf /path-to-tar/ycsb-tikv-binding-0.18.0-SNAPSHOT.tar.gz
cd ./ycsb-tikv-binding-0.18.0-SNAPSHOT
./bin/ycsb.sh run tikv -s [-threads <thread-num>] [-P <tikv.properties>] [-P <path-to-workload>]
```

## tikv.properties

|property name|description|default value|
|---|---|---|
|tikv.pd.addresses|pd addresses(with port) separated by comma|127.0.0.1:2379|
