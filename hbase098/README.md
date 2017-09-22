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

# HBase (0.98.x) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a HBase 0.98.x Server cluster.
To run against an HBase >= 1.0 cluster, use the `hbase10` binding.

## Quickstart

### 1. Start a HBase Server
You need to start a single node or a cluster to point the client at. Please see [Apache HBase Reference Guide](http://hbase.apache.org/book.html) for more details and instructions.

### 2. Set up YCSB
You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn clean package
```

### 3. Create a HBase table for testing

For best results, use the pre-splitting strategy recommended in [HBASE-4163](https://issues.apache.org/jira/browse/HBASE-4163):

```
hbase(main):001:0> n_splits = 200 # HBase recommends (10 * number of regionservers)
hbase(main):002:0> create 'usertable', 'family', {SPLITS => (1..n_splits).map {|i| "user#{1000+i*(9999-1000)/n_splits}"}}
```

*Failing to do so will cause all writes to initially target a single region server*.

### 4. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

You should specify a HBase config directory(or any other directory containing your hbase-site.xml) and a table name and a column family(-cp is used to set java classpath and -p is used to set various properties).

```
bin/ycsb load hbase -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family
```

Then, you can run the workload:

```
bin/ycsb run hbase -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply additional properties (as seen in the next section) like this:

```
bin/ycsb run hbase -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family -p clientbuffering=true
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
* `writebuffersize`: The maximum amount, in bytes, of data to buffer on the client side before a flush is forced. The default is 12MB.

Additional HBase settings should be provided in the `hbase-site.xml` file located in your `/HBASE-HOME-DIR/conf` directory. Typically this will be `/etc/hbase/conf`.
