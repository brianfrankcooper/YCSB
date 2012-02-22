# Building YCSB

To build YCSB, run:

    mvn clean package

# Running YCSB

Once `mvn clean package` succeeds, you can run `ycsb` command:

    ./bin/ycsb load basic workloads/workloada
    ./bin/ycsb run basic workloads/workloada

# Oracle NoSQL Database

Oracle NoSQL Database binding doesn't get built by default because there is no
Maven repository for it. To build the binding:

1. Download kv-ce-1.2.123.tar.gz from here:

    http://www.oracle.com/technetwork/database/nosqldb/downloads/index.html

2. Untar kv-ce-1.2.123.tar.gz and install kvclient-1.2.123.jar in your local
   maven repository:

    tar xfvz kv-ce-1.2.123.tar.gz
    mvn install:install-file -Dfile=kv-1.2.123/lib/kvclient-1.2.123.jar \
        -DgroupId=com.oracle -DartifactId=kvclient -Dversion=1.2.123
        -Dpackaging=jar

3. Uncomment `<module>nosqldb</module>` and run `mvn clean package`.
