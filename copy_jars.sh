#!/bin/bash
db=$1

if [ -z $1 ]; then
    echo "db - redis or mongodb not provided"
    exit 1
fi

DEST_DIR="./jars/"
mkdir -p "$DEST_DIR"

if [ $1 = "redis"]; then
    CLASSPATH="/home/snayyer/repos/non-work/studium/thesis/YCSB/redis/conf:/home/snayyer/repos/non-work/studium/thesis/YCSB/redis/target/redis-binding-0.18.0-SNAPSHOT.jar:/home/snayyer/.m2/repository/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar:/home/snayyer/.m2/repository/com/google/protobuf/protobuf-java/3.19.6/protobuf-java-3.19.6.jar:/home/snayyer/.m2/repository/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar:/home/snayyer/.m2/repository/redis/clients/jedis/2.9.0/jedis-2.9.0.jar:/home/snayyer/.m2/repository/org/apache/commons/commons-pool2/2.4.2/commons-pool2-2.4.2.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar:/home/snayyer/repos/non-work/studium/thesis/YCSB/core/target/core-0.18.0-SNAPSHOT.jar"
elif [ $1 = "mongodb" ]; then
    CLASSPATH="/home/snayyer/repos/non-work/studium/thesis/YCSB/mongodb/conf:/home/snayyer/repos/non-work/studium/thesis/YCSB/mongodb/target/mongodb-binding-0.18.0-SNAPSHOT.jar:/home/snayyer/.m2/repository/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar:/home/snayyer/.m2/repository/com/google/protobuf/protobuf-java/3.19.6/protobuf-java-3.19.6.jar:/home/snayyer/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.1/snappy-java-1.1.7.1.jar:/home/snayyer/.m2/repository/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar:/home/snayyer/.m2/repository/org/mongodb/mongo-java-driver/3.11.0/mongo-java-driver-3.11.0.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar:/home/snayyer/repos/non-work/studium/thesis/YCSB/core/target/core-0.18.0-SNAPSHOT.jar:/home/snayyer/.m2/repository/com/allanbank/mongodb-async-driver/2.0.1/mongodb-async-driver-2.0.1.jar"
else
    echo "Unknown db - neither redis nor mongodb"
    exit 1
fi

IFS=':' read -ra JARS <<< "$CLASSPATH"
for JAR in "${JARS[@]}"; do
    if [ -e "$JAR" ]; then
        cp "$JAR" "$DEST_DIR"
    else
        echo "Skipping $JAR as it does not exist."
    fi
done

scp ./jars/* snayyer@zs01:/home/snayyer/YCSB/jars