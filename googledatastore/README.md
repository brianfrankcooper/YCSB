<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2015 YCSB contributors.
All rights reserved.

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

# Google Cloud Datastore Binding

https://cloud.google.com/datastore/docs/concepts/overview?hl=en

## Configure

    YCSB_HOME - YCSB home directory
    DATASTORE_HOME - Google Cloud Datastore YCSB client package files

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Using-the-Database-Libraries for more information on setup.

# Benchmark

    $YCSB_HOME/bin/ycsb load googledatastore -P workloads/workloada -P googledatastore.properties
    $YCSB_HOME/bin/ycsb run googledatastore -P workloads/workloada -P googledatastore.properties

# Properties

    $DATASTORE_HOME/conf/googledatastore.properties

# FAQs

