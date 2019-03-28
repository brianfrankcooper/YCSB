<!--
Copyright (c) 2015-2017 YCSB contributors. All rights reserved.

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

# HBase (1.4+) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a HBase 1.4+ Server cluster, using a shaded client that tries to avoid leaking third party libraries.

See `hbase098/README.md` for a quickstart to setup HBase for load testing and common configuration details.

## Configuration Options
In addition to those options available for the `hbase098` binding, the following options are available for the `hbase14` binding:

* `durability`: Whether or not writes should be appended to the WAL. Bypassing the WAL can improve throughput but data cannot be recovered in the event of a crash. The default is true.

