#!/bin/bash

# Directory to copy JAR files to
DEST_DIR="./jars/"
# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"
# Classpath extracted from the Python script
CLASSPATH="/home/snayyer/repos/non-work/studium/thesis/YCSB/redis/conf:/home/snayyer/repos/non-work/studium/thesis/YCSB/redis/target/redis-binding-0.18.0-SNAPSHOT.jar:/home/snayyer/.m2/repository/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar:/home/snayyer/.m2/repository/com/google/protobuf/protobuf-java/3.19.6/protobuf-java-3.19.6.jar:/home/snayyer/.m2/repository/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar:/home/snayyer/.m2/repository/redis/clients/jedis/2.9.0/jedis-2.9.0.jar:/home/snayyer/.m2/repository/org/apache/commons/commons-pool2/2.4.2/commons-pool2-2.4.2.jar:/home/snayyer/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar:/home/snayyer/repos/non-work/studium/thesis/YCSB/core/target/core-0.18.0-SNAPSHOT.jar"

# Copy each JAR file to the destination directory
IFS=':' read -ra JARS <<< "$CLASSPATH"
for JAR in "${JARS[@]}"; do
    if [ -e "$JAR" ]; then
        cp "$JAR" "$DEST_DIR"
    else
        echo "Skipping $JAR as it does not exist."
    fi
done

scp ./jars/* snayyer@zs01:/home/snayyer/YCSB/jars