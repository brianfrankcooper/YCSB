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

# HBase (1.y) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a HBase 1 cluster, using a shaded client that tries to avoid leaking third party libraries.

# Testing HBase
## 1. Start a HBase Server
You need to start a single node or a cluster to point the client at. Please see [Apache HBase Reference Guide](http://hbase.apache.org/book.html) for more details and instructions.

## 2. Set up YCSB

Download the [latest YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest) file. Follow the instructions.

## 3. Create a HBase table for testing

For best results, use the pre-splitting strategy recommended in [HBASE-4163](https://issues.apache.org/jira/browse/HBASE-4163):

```
hbase(main):001:0> n_splits = 200 # HBase recommends (10 * number of regionservers)
hbase(main):002:0> create 'usertable', 'family', {SPLITS => (1..n_splits).map {|i| "user#{1000+i*(9999-1000)/n_splits}"}}
```

*Failing to do so will cause all writes to initially target a single region server*.

## 4. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

You should specify a HBase config directory(or any other directory containing your hbase-site.xml) and a table name and a column family(-cp is used to set java classpath and -p is used to set various properties).

```
bin/ycsb load hbase1 -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family
```

Then, you can run the workload:

```
bin/ycsb run hbase1 -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply additional properties (as seen in the next section) like this:

```
bin/ycsb run hbase1 -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family -p clientbuffering=true
```

## Configuration Options
Following options can be configurable using `-p`.

* `columnfamily`: The HBase column family to target.
* `debug` : If true, debugging logs are activated. The default is false.
* `hbase.usepagefilter` : If true, HBase
  [PageFilter](https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/PageFilter.html)s
  are used to limit the number of records consumed in a scan operation. The default is true.
* `principal`: If testing need to be done against a secure HBase cluster using Kerberos Keytab,
  this property can be used to pass the principal in the keytab file.
* `keytab`: The Kerberos keytab file name and location can be passed through this property.
* `clientbuffering`: Whether or not to use client side buffering and batching of write operations. This can significantly improve performance and defaults to true.
* `writebuffersize`: The maximum amount, in bytes, of data to buffer on the client side before a flush is forced. The default is 12MB. Only used when `clientbuffering` is true.
* `durability`: Whether or not writes should be appended to the WAL. Bypassing the WAL can improve throughput but data cannot be recovered in the event of a crash. The default is true.

Additional HBase settings should be provided in the `hbase-site.xml` file located in your `/HBASE-HOME-DIR/conf` directory. Typically this will be `/etc/hbase/conf`.

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
bin/ycsb load hbase1 -p columnfamily=cf -cp 'target/bigtable-deps/*' -P workloads/workloada

```

The `load` step only executes inserts into the datastore. After loading data, run the same workload to mix reads with writes.

```
bin/ycsb run hbase1 -p columnfamily=cf -cp 'target/bigtable-deps/* -P workloads/workloada

```
