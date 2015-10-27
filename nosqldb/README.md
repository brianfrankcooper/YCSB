CONFIGURE

$KVHOME is Oracle NoSQL Database package files.
$KVROOT is a data directory.
$YCSBHOME is a YCSB home directory.

    mkdir $KVROOT
    java -jar $KVHOME/lib/kvstore-1.2.123.jar makebootconfig \
       -root $KVROOT -port 5000 -admin 5001 -host localhost \
       -harange 5010,5020
    java -jar $KVHOME/lib/kvstore-1.2.123.jar start -root $KVROOT
    java -jar $KVHOME/lib/kvstore-1.2.123.jar runadmin \
        -port 5000 -host localhost -script $YCSBHOME/conf/script.txt

BENCHMARK

    $YCSBHOME/bin/ycsb load nosqldb -P workloads/workloada
    $YCSBHOME/bin/ycsb run nosqldb -P workloads/workloada

PROPERTIES

See $YCSBHOME/conf/nosqldb.properties.

STOP

$ java -jar $KVHOME/lib/kvstore-1.2.123.jar stop -root $KVROOT


Please refer to Oracle NoSQL Database docs here:
http://docs.oracle.com/cd/NOSQL/html/index.html
