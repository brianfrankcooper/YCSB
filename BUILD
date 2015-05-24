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

# Mapkeeper 

Mapkeeper binding doesn't get built by default because there is no
Maven repository for it. To build the binding:

1. follow "https://github.com/m1ch1/mapkeeper/wiki/Getting-Started" to Generate Thrift MapKeeper Binding
   a.  install libevent
   b.  install boost
   c.  install thrift
   d.  generate mapkeeper binding

2. After generating mapkeeper binding, go the "mapkeeper/thrift/gen-java/target", and 
   cp mapkeeper-1.1-SNAPSHOT.jar mapkeeper-1.0.jar 
   mvn install:install-file -Dfile=mapkeeper-1.0.jar -DgroupId=com.yahoo.mapkeeper -DartifactId=mapkeeper -Dversion=1.0 -Dpackaging=jar

3. Modify "YCSB/mapkeeper/pom.xml" as follows:
    <dependency>
           <groupId>com.yahoo.mapkeeper</groupId>
           <artifactId>mapkeeper</artifactId>
           <version>1.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.thrift</groupId>
        <artifactId>libthrift</artifactId>
        <version>0.8.0</version>
    </dependency>


4. Uncomment "<module>mapkeeper</module>" and "<mapkeeper.version>1.0</mapkeeper.version>" in "YCSB/pom.xml"

4. mvn clean package
