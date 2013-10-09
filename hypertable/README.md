## Quick Start


### Before you get started -- a brief note about the ThriftBroker

The Hypertable _ThriftBroker_ process is what allows Java clients to communicate
with Hypertable and should be installed on all YCSB client machines.  The
Hypertable YCSB driver expects there to be a ThriftBroker running on localhost,
to which it connects.  A ThriftBroker will automatically be started on all
Hypertable machines that run either a Master or RangeServer.  If you plan to run
the ycsb command script on machines that are **not** running either a Master or
RangeServer, then you'll need to install Hypertable on those machines and
configure them to start ThriftBrokers.  To do that, when performing step 3 of
the installation instructions below, list these additional client machines under
the :thriftbroker_additional role in the Capfile.  For example:

    role :thriftbroker_additional, ycsb-client00, ycsb-client01, ...


### Step 1. Install Hypertable

See [Hypertable Installation Guide]
(http://hypertable.com/documentation/installation/quick_start_cluster_installation/)
for the complete set of install instructions.


### Step 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn clean package


### Step 3. Run Hypertable

Once it has been installed, start Hypertable by running the following command in
the directory containing your Capfile:

    cap start

Then create the _ycsb_ namespace and a table called _usertable_:

    echo "create namespace ycsb;" | /opt/hypertable/current/bin/ht shell --batch
    echo "use ycsb; create table usertable (family);" | /opt/hypertable/current/bin/ht shell --batch

All interactions by YCSB take place under the Hypertable namespace *ycsb*.
Hypertable also uses an additional data grouping structure called a column
family that must be set. YCSB doesn't offer fine grained operations on column
families so in this example the table is created with a single column family
named _family_ to which all column families will belong.  The name of this
column family must be passed to YCSB. The table can be manipulated from within
the hypertable shell without interfering with the operation of YCSB. 


### Step 4. Run YCSB

Make sure that an instance of Hypertable is running. To access the database
through the YCSB shell, from the YCSB directory run:

    ./bin/ycsb shell hypertable -p columnfamily=family

where the value passed to columnfamily matches that used in the table
creation. To run a workload, first load the data:

    ./bin/ycsb load hypertable -P workloads/workloada -p columnfamily=family

Then run the workload:

    ./bin/ycsb run hypertable -P workloads/workloada -p columnfamily=family

This example runs the core workload _workloada_ that comes packaged with YCSB.
The state of the YCSB data in the Hypertable database can be reset by dropping
usertable and recreating it.
