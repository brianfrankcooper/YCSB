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

# Crate Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Crate Server cluster. It uses the official Crate JDBC and provides a rich set of configuration options.

# Running tests

Preferrable is to run using docker

docker pull crate

docker run -d -p 4200:4200 -p 4300:4300 crate crate

Build module:

mvn -pl com.yahoo.ycsb:crate-binding -am clean package

Run:

./bin/ycsb load crate -P workloads/workloada -P crate/src/main/conf/db.properties

