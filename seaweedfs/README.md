<!--
Copyright (c) 2020 YCSB contributors. All rights reserved.

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
Quick Start
===============
[SeaweedFS](https://github.com/chrislusf/seaweedfs) is a distributed file system with optimization for small files.
It can also be used as a key-value store for large values.

### 1. Set Up YCSB

Download the YCSB from this website:

    https://github.com/brianfrankcooper/YCSB/releases/

You can choose to download either the full stable version or just one of the available binding.

### 2. Run YCSB

To execute the benchmark using the SeaweedFS storage binding, first files must be uploaded using the "load" option with 
this command:

       ./bin/ycsb load seaweedfs -p seaweed.filerHost=localhost -p seaweed.filerPort=8888 -p seaweed.folder=/ycsb -p fieldlength=10 -p fieldcount=20 -p recordcount=10000 -P workloads/workloada

With this command, the workload A will be executing with the loading phase. The file size is determined by the number 
of fields (fieldcount) and by the field size (fieldlength). In this case each file is 200 bytes (10 bytes for each 
field multiplied by 20 fields).

Running the command:

       ./bin/ycsb run seaweedfs -p seaweed.filerHost=localhost -p seaweed.filerPort=8888 -p seaweed.folder=/ycsb -p fieldlength=10 -p fieldcount=20 -p recordcount=10000 -P workloads/workloada

the workload A will be executed with file size 200 bytes. 

#### SeaweedFS Storage Configuration Parameters

- `seaweed.filerHost`
  - This indicate the filer host or ip address.
  - Default value is `localhost`.

- `seaweed.filerPort`
  - This indicate the filer port.
  - Default value is `8888`.

- `seaweed.folder`
  - This indicate the folder on filer to store all the files.
  - Default value is `/ycsb`.
