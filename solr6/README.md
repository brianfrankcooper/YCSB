<!--
Copyright (c) 2016 YCSB contributors. All rights reserved.

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

This section describes how to run YCSB on Solr running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:solr6-binding -am clean package

### 2. Set Up Solr

There must be a running Solr instance with a core/collection pre-defined and configured. 
- See this [API](https://cwiki.apache.org/confluence/display/solr/CoreAdmin+API#CoreAdminAPI-CREATE) reference on how to create a core.
- See this [API](https://cwiki.apache.org/confluence/display/solr/Collections+API#CollectionsAPI-api1) reference on how to create a collection in SolrCloud mode.

The `conf/schema.xml` configuration file present in the core/collection just created must be configured to handle the expected field names during benchmarking.
Below illustrates a sample from a schema config file that matches the default field names used by the ycsb client:

	<field name="id" type="string" indexed="true" stored="true" required="true" multiValued="false"/>
	<field name="field0" type="text_general" indexed="true" stored="true"/>
	<field name="field1" type="text_general" indexed="true" stored="true"/>
	<field name="field2" type="text_general" indexed="true" stored="true"/>
	<field name="field3" type="text_general" indexed="true" stored="true"/>
	<field name="field4" type="text_general" indexed="true" stored="true"/>
	<field name="field5" type="text_general" indexed="true" stored="true"/>
	<field name="field6" type="text_general" indexed="true" stored="true"/>
	<field name="field7" type="text_general" indexed="true" stored="true"/>
	<field name="field8" type="text_general" indexed="true" stored="true"/>
	<field name="field9" type="text_general" indexed="true" stored="true"/>

If running in SolrCloud mode ensure there is an external Zookeeper cluster running.
- See [here](https://cwiki.apache.org/confluence/display/solr/Setting+Up+an+External+ZooKeeper+Ensemble) for details on how to set up an external Zookeeper cluster.
- See [here](https://cwiki.apache.org/confluence/display/solr/Using+ZooKeeper+to+Manage+Configuration+Files) for instructions on how to use Zookeeper to manage your core/collection configuration files.

### 3. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load solr6 -s -P workloads/workloada -p table=<core/collection name>

Then, run the workload:

    ./bin/ycsb run solr6 -s -P workloads/workloada -p table=<core/collection name>

For further configuration see below: 

### Default Configuration Parameters
The default settings for the Solr node that is created is as follows:
	
- `solr.cloud` 
  - A Boolean value indicating if Solr is running in SolrCloud mode. If so there must be an external Zookeeper cluster running also.
  - Default value is `false` and therefore expects solr to be running in stand-alone mode.

- `solr.base.url` 
  - The base URL in which to interface with a running Solr instance in stand-alone mode
  - Default value is `http://localhost:8983/solr

- `solr.commit.within.time`
  - The max time in ms to wait for a commit when in batch mode, ignored otherwise
  - Default value is `1000ms`

- `solr.batch.mode`
  - Indicates if inserts/updates/deletes should be commited in batches (frequency controlled by the `solr.commit.within.time` parameter) or commit 1 document at a time.
  - Default value is `false`

- `solr.zookeeper.hosts`
  - A list of comma seperated host:port pairs of Zookeeper nodes used to manage SolrCloud configurations.
  - Must be passed when in [SolrCloud](https://cwiki.apache.org/confluence/display/solr/SolrCloud) mode.
  - Default value is `localhost:2181`

### Custom Configuration
If you wish to customize the settings used to create the Solr node
you can created a new property file that contains your desired Solr 
node settings and pass it in via the parameter to 'bin/ycsb' script. Note that 
the default properties will be kept if you don't explicitly overwrite them.

Assuming that we have a properties file named "myproperties.data" that contains 
custom Solr node configuration you can execute the following to
pass it into the Solr client:

    ./bin/ycsb run solr6 -P workloads/workloada -P myproperties.data -s

If you wish to use SolrCloud mode ensure a Solr cluster is running with an
external zookeeper cluster and an appropriate collection has been created.
Make sure to pass the following properties as parameters to 'bin/ycsb' script.

	solr.cloud=true
	solr.zookeeper.hosts=<zkHost2>:<zkPort1>,...,<zkHostN>:<zkPortN>


