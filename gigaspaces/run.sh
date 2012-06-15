#!/bin/bash

# see readme.txt for details how to run the benchmark
export CLIENT_DRIVER=com.yahoo.ycsb.db.GigaSpacesClient
export NIC_ADDR=10.10.10.249
export db=com.yahoo.ycsb.db.GigaSpacesClient
export OUTPUT_DIR=../YCSB_RESULTS/GigaSpacesReadWriteTPBenchResults_MAR_29_SINGLE_INDEXED-V3

# local cache URL
#export gigspaces_url="jini://10.10.10.209/*/mySpace?useLocalCache"

# Remote space URL
export gigspaces_url="jini://10.10.10.209/*/mySpace"

#export db=com.yahoo.ycsb.db.GemFireClient
#export OUTPUT_DIR=../YCSB_RESULTS/GemFireReadWriteTPBenchResults_MAR_29_SINGLE_INDEXED-V2

########################################
export JAVA_HOME=/usr/local/lib/jdk/jdk7u3

export GS_HOME=/export/home/shay/xap9m5
export GF_HOME=../vFabric_GemFire_661

export GC_ARGS="-Xmx1G -Xms1G -Djava.rmi.server.hostname=$NIC_ADDR"

export GS_JARS=$GF_HOME/lib/gemfire.jar:$GS_HOME/lib/required/*

mkdir $OUTPUT_DIR
rm -f $OUTPUT_DIR/*

recordcount=1000
operationcount=1000

for i in 1 2 3 4 5 6 7 8
do
#   echo "Benchmarking $i target TP"
   	echo "Benchmarking $i Threads"

	export TP="$i"
	export ARG="$i"

	#single mode
	recordcount=$((ARG * 40000))
	operationcount=$((ARG * 40000))


#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P workloadRemoteCacheWriteOnly -threads $ARG -l "$ARG Thread" -p recordcount=$recordcount  -p operationcount=$operationcount  -p db=$CLIENT_DRIVER -p collectlatencystats=false -p cachevalues=true 

# 	Remote Space
#	readrepeatcount=$((ARG * 4))
	readrepeatcount=1

# 	Local Cache
#	readrepeatcount=$((ARG * 500))

	echo readrepeatcount  = $readrepeatcount 
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG  -p measurementtype=timeseries -p timeseries.granularity=2000 -target $TP -l $TP > "out/$TP.txt"
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG -l "$ARG Thread"  -p collectlatencystats=true -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p collectlatencystats=false -p gigspaces.url=$gigspaces_url -p gemfireclientconfig=/root/shay/YCSB/gemfire/src/main/conf/client.xml > "$OUTPUT_DIR/$ARG Threads.txt" 2>&1

# 	echo single write 
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P workloadRemoteCacheReadOnly -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true > "$OUTPUT_DIR/$ARG WRITE-Threads.txt" 2>&1

# 	echo single read
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml > "$OUTPUT_DIR/$ARG READ-Threads.txt" 2>&1

    echo single Indexed write 
    $JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P workloadRemoteCacheReadOnly -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount -p clearbeforetest=true -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG WRITE-SINGLE_INDEXED-Threads.txt" 2>&1

	echo single Indexed read
	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index  > "$OUTPUT_DIR/$ARG READ-SINGLE_INDEXED-Threads.txt" 2>&1

	#batch mode
#	recordcount=$((ARG * 10000))
#	operationcount=$((ARG * 10000))

# 	echo batch write regular
#   $JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P workloadRemoteCacheReadOnly -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true -p batchmode=true -p batchsize=100 -p payloadtype=regular > "$OUTPUT_DIR/$ARG Threads-WRITE-BATCH-REGUALR.txt" 2>&1

# 	echo batch read regular
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p batchmode=true -p batchsize=100 -p clearbeforetest=false -p payloadtype=regular -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml > "$OUTPUT_DIR/$ARG Threads-READ_BATCH-REGUALR.txt" 2>&1

#	echo batch write Indexed
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P workloadRemoteCacheReadOnly -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true -p batchmode=true -p batchsize=100 -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG Threads-WRITE-BATCH-Indexed.txt" 2>&1

#	echo batch read Indexed
#	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P workloadRemoteCacheReadOnly -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p batchmode=true -p batchsize=100 -p clearbeforetest=false -p payloadtype=indexed -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG Threads-READ_BATCH-Indexed.txt" 2>&1

done
