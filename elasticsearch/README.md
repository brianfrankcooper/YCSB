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

This section describes how to run YCSB on ElasticSearch running locally. 

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
The default setting for the ElasticSearch node that is created is as follows:

    cluster.name=es.ycsb.cluster
    node.local=true
    path.data=$TEMP_DIR/esdata
    discovery.zen.ping.multicast.enabled=false
    index.mapping._id.indexed=true
    index.gateway.type=none
    gateway.type=none
    index.number_of_shards=1
    index.number_of_replicas=0
    es.index.key=es.ycsb

### Custom Configuration
If you wish to customize the settings used to create the ElasticSerach node
you can created a new property file that contains your desired ElasticSearch 
node settings and pass it in via the parameter to 'bin/ycsb' script. Note that 
the default properties will be kept if you don't explicitly overwrite them.

Assuming that we have a properties file named "myproperties.data" that contains 
custom ElasticSearch node configuration you can execute the following to
pass it into the ElasticSearch client:


    ./bin/ycsb run elasticsearch -P workloads/workloada -P myproperties.data -s


If you wish to use a in-memory store type rather than the default disk store add 
the following properties to your custom properties file. For a large number of 
insert operations insure that you have sufficient memory on your test system 
otherwise you will run out of memory.

    index.store.type=memory
    index.store.fs.memory.enabled=true
    cache.memory.small_buffer_size=4mb
    cache.memory.large_cache_size=1024mb

If you wish to change the default index name you can set the following property:

    es.index.key=my_index_key
