<!--
Copyright (c) 2015-2016 YCSB contributors. All rights reserved.

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

# HBase (1.0.x) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a HBase 1.0.x Server cluster or Google's hosted Bigtable.
To run against an HBase 0.98.x cluster, use the `hbase098` binding.

See `hbase098/README.md` for a quickstart to setup HBase for load testing and common configuration details.

## Configuration Options
In addition to those options available for the `hbase098` binding, the following options are available for the `hbase10` binding:

* `durability`: Whether or not writes should be appended to the WAL. Bypassing the WAL can improve throughput but data cannot be recovered in the event of a crash. The default is true.

## Bigtable

Google's Bigtable service provides an implementation of the HBase API for migrating existing applications. Users can perform load tests against Bigtable using this binding.

### 1. Setup a Bigtable Cluster

Login to the Google Cloud Console and follow the [Creating Cluster](https://cloud.google.com/bigtable/docs/creating-cluster) steps. Make a note of your cluster name, zone and project ID.

### 2. Launch the Bigtable Shell

From the Cloud Console, launch a shell and follow the [Quickstart](https://cloud.google.com/bigtable/docs/quickstart) up to step 4 where you launch the HBase shell.

### 3. Create a Table

For best results, use the pre-splitting strategy recommended in [HBASE-4163](https://issues.apache.org/jira/browse/HBASE-4163):

```
hbase(main):001:0> n_splits = 200 # HBase recommends (10 * number of regionservers)
hbase(main):002:0> create 'usertable', 'cf', {SPLITS => (1..n_splits).map {|i| "user#{1000+i*(9999-1000)/n_splits}"}}
```

Make a note of the column family, in this example it's `cf``.

### 4. Download the Bigtable Client Jar with required dependencies:

```
mvn -N dependency:copy -Dartifact=com.google.cloud.bigtable:bigtable-hbase-1.x-hadoop:1.0.0 -DoutputDirectory=target/bigtable-deps
mvn -N dependency:copy -Dartifact=io.dropwizard.metrics:metrics-core:3.1.2 -DoutputDirectory=target/bigtable-deps
```

Download the latest `bigtable-hbase-1.x-hadoop` jar from [Maven](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.google.cloud.bigtable%22%20AND%20a%3A%22bigtable-hbase-1.x-hadoop%22) to your host.

### 5. Download JSON Credentials

Follow these instructions for [Generating a JSON key](https://cloud.google.com/bigtable/docs/installing-hbase-shell#service-account) and save it to your host.

### 6. Create or Edit hbase-site.xml

If you have an existing HBase configuration directory with an `hbase-site.xml` file, edit the file as per below. If not, create a directory called `conf` under the `hbase10` directory. Create a file in the conf directory named `hbase-site.xml`. Provide the following settings in the XML file, making sure to replace the bracketed examples with the proper values from your Cloud console.

```
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
  </property>
  <property>
    <name>google.bigtable.project.id</name>
    <value>[YOUR-PROJECT-ID]</value>
  </property>
  <property>
    <name>google.bigtable.instance.id</name>
    <value>[YOUR-INSTANCE-ID]</value>
  </property>
  <property>
    <name>google.bigtable.auth.service.account.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>google.bigtable.auth.json.keyfile</name>
    <value>[PATH-TO-YOUR-KEY-FILE]</value>
  </property>
</configuration>
```

If you have an existing HBase config directory, make sure to add it to the class path via `-cp <PATH_TO_BIGTABLE_JAR>:<CONF_DIR>`.

### 7. Execute a Workload

Switch to the root of the YCSB repo and choose the workload you want to run and `load` it first. With the CLI you must provide the column family, cluster properties and the ALPN jar to load.

```
bin/ycsb load hbase10 -p columnfamily=cf -cp 'target/bigtable-deps/*' -P workloads/workloada

```

The `load` step only executes inserts into the datastore. After loading data, run the same workload to mix reads with writes.

```
bin/ycsb run hbase10 -p columnfamily=cf -cp 'target/bigtable-deps/* -P workloads/workloada

```
