<!--
Copyright (c) 2019 YCSB contributors.
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

# Google Cloud Firestore Binding

https://cloud.google.com/firestore/

Please refer [here](https://cloud.google.com/firestore/docs/apis) for more information on
Google Cloud Firestore API.

## Configure

    YCSB_HOME - YCSB home directory
    FIRESTORE_HOME - Google Cloud Firestore YCSB client package files

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Using-the-Database-Libraries
for more information on setup.

# Benchmark

    $YCSB_HOME/bin/ycsb load googlefirestore -P workloads/workloada -P googlefirestore.properties
    $YCSB_HOME/bin/ycsb run googlefirestore -P workloads/workloada -P googlefirestore.properties

# Properties

    $FIRESTORE_HOME/conf/googlefirestore.properties

# Details

A. Configuration and setup:

See this link for instructions about setting up Google Cloud Firestore and
authentication:

https://cloud.google.com/firestore/docs/quickstart-servers

Save key in JSON format in root folder, name it serviceAccountKey.json.
