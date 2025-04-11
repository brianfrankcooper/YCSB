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

## Quick Start

This section describes how to run YCSB on Elasticsearch 7.x running locally. 

### 1. Install and Start Elasticsearch

[Download and install Elasticsearch][1]. When starting Elasticsearch, you should
[configure][2] the cluster name to be `es.ycsb.cluster` (see below).

#### 1.1 Testing with Docker

Note: For the sake of the demo we're using elastic7.9 to avoid the `Elasticsearch built-in security features are not enabled` log lines.

```bash
docker run -p 9200:9200 -p 9300:9300 \
       -e "discovery.type=single-node" \
       docker.elastic.co/elasticsearch/elasticsearch:7.9.2
```

### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:elasticsearch7-binding -am clean package

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load elasticsearch7-rest -s -P workloads/workloada

You can easily confirm the index status via the REST API (adjust to your conn details):

    $ curl http://localhost:9200/es.ycsb/_count?pretty
    {
      "count" : 1000,
      "_shards" : {
        "total" : 1,
        "successful" : 1,
        "skipped" : 0,
        "failed" : 0
      }
    }

Then, run the workload:

    ./bin/ycsb run elasticsearch7-rest -s -P workloads/workloada


The Elasticsearch 7 binding requires a standalone instance of Elasticsearch.
You must specify a hosts list for the transport client to connect to via
`-p es.hosts.list=<hostname1:port1>,...,<hostnamen:portn>`:

    ./bin/ycsb run elasticsearch7-rest -s -P workloads/workloada \
    -p es.hosts.list=<hostname1:port1>,...,<hostnamen:portn>`

Note that `es.hosts.list` defaults to `localhost:9300`. For further
configuration see below:

### Defaults Configuration
The default setting for the Elasticsearch node that is created is as follows:

    es.setting.cluster.name=es.ycsb.cluster
    es.index.key=es.ycsb
    es.number_of_shards=1
    es.number_of_replicas=0
    es.new_index=false
    es.queries.cache.enabled=true
    es.refresh_interval=1s
    es.hosts.list=localhost:9200
    es.user=""
    es.password=""
    es.hosts.scheme="http"

### Custom Configuration
If you wish to customize the settings used to create the Elasticsearch node
you can created a new property file that contains your desired Elasticsearch 
node settings and pass it in via the parameter to 'bin/ycsb' script. Note that 
the default properties will be kept if you don't explicitly overwrite them.

Assuming that we have a properties file named "myproperties.data" that contains 
custom Elasticsearch node configuration you can execute the following to
pass it into the Elasticsearch client:

    ./bin/ycsb run elasticsearch7 -P workloads/workloada -P myproperties.data -s

If you wish to change the default index name you can set the following property:

    es.index.key=my_index_key

By default this will use localhost:9300 as a seed node to discover the cluster.
You can also specify

    es.hosts.list=(\w+:\d+)+

(a comma-separated list of host/port pairs) to change this.

[1]: https://www.elastic.co/guide/en/elasticsearch/reference/7.2/install-elasticsearch.html
[2]: https://www.elastic.co/guide/en/elasticsearch/reference/7.2/setup.html
