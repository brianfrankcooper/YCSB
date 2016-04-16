<!--
Copyright (c) 2012 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Elasticsearch running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load elasticsearch -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run elasticsearch -s -P workloads/workloada

For further configuration see below: 

### Defaults Configuration
The default setting for the Elasticsearch node that is created is as follows:

    cluster.name=es.ycsb.cluster
    es.index.key=es.ycsb
    es.number_of_shards=1
    es.number_of_replicas=0
    es.remote=false
    es.newdb=false
    es.hosts.list=localhost:9200 (only applies if es.remote=true)

### Custom Configuration
If you wish to customize the settings used to create the Elasticsearch node
you can created a new property file that contains your desired Elasticsearch 
node settings and pass it in via the parameter to 'bin/ycsb' script. Note that 
the default properties will be kept if you don't explicitly overwrite them.

Assuming that we have a properties file named "myproperties.data" that contains 
custom Elasticsearch node configuration you can execute the following to
pass it into the Elasticsearch client:


    ./bin/ycsb run elasticsearch -P workloads/workloada -P myproperties.data -s

If you wish to change the default index name you can set the following property:

    es.index.key=my_index_key

If you wish to run against a remote cluster you can set the following property:

    es.remote=true

By default this will use localhost:9300 as a seed node to discover the cluster.
You can also specify

    es.hosts.list=(\w+:\d+)+

(a comma-separated list of host/port pairs) to change this.
