# HBase Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a HBase Server cluster.

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

### 5. Kerberos
For secure Hadoop, you could enable kerberos authentication option.

```
kinit -kt /path/to/keytab user@XXX.COM
bin/ycsb run hbase -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family -p kerberos=true
```

Or specify kerberos principle and keytab as options:

```
bin/ycsb run hbase -P workloads/workloada -cp /HBASE-HOME-DIR/conf -p table=usertable -p columnfamily=family -p kerberos=true -p kerberosuser=user@XXX.COM -p kerberoskeytab=/path/to/keytab
```

## Configuration Options
Following options can be configurable using `-p`.

* `columnfamily`: The HBase column family to target.
* `clientbuffering` : If true, buffer mutations on the client. The default is false.
* `writebuffersize` : Buffer size to be used when `clientbuffering` is activated. The default is 12MB.
* `debug` : If true, debugging logs are activated. The default is false.
* `kerberos` : If true, use Kerberos authentication.
* `kerberosuser` : Kerberos principle [optional].
* `kerberoskeytab`: Kerberos keytab path [optional].
