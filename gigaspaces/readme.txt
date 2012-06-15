March 2012
----------------------------------------
This readme is part of the Yahoo cloud benchmark. It allows you to compare GigaSpaces and Gemfire performance in various scenarios. The readme provide info about :
- how to start GigaSpaces
- how to start GemFire
- How to run the benchmark
- How to analyze the results

To run GigaSpaces Space Server:
##################################################
- Install GigaSpaces
- set JAVA_HOME as the JDK home folder 
- set NIC_ADDR as the machine IP
- set COMPONENT_JAVA_OPTIONS to have relevant JVM arguments. Example: export COMPONENT_JAVA_OPTIONS="-Xmx15g -Xms15g"
- run <GigaSpaces root>/bin/gsInstance.sh

To run vFabric GemFire Server:
##################################################
Install GemFire 
- set JAVA_HOME as the JDK home folder
- set CLASSPATH to include the benchmark jars
- For regular payload benchmarks: run <GigaSpaces root>/bin/cacheserver start cache-xml-file=/YCSB/gemfire/src/main/conf/cache.xml
- For indexed payload benchmarks: run <GigaSpaces root>/bin/cacheserver start cache-xml-file=/YCSB/gemfire/src/main/conf/cacheindexed.xml
- For P2P benchmarks: run <GigaSpaces root>/bin/cacheserver start cache-xml-file=/YCSB/gemfire/src/main/conf/cachep2p.xml

Client Benchmark execution script
# Supported Variables
##################################################
- OUTPUT_DIR - Output folder
- NIC_ADDR - Client Machine IP
- GS_HOME - GigaSpaces home folder
- GC_ARGS - Client JVM args. Example: -Xmx1G -Xms1G 
- GF_HOME - GemFire home folder
- JAVA_HOME - Java home folder
- CLIENT_DRIVER - The benchmark client implementation. You can have com.yahoo.ycsb.db.GigaSpacesClient , or com.yahoo.ycsb.db.GemFireClient
- GemFire client config file location

Supported options
##################################################
- threads - Number of Client threads
- gigspaces.url=jini://localhost/*/mySpace for remoete space , /./mySpace for embedded space , jini://localhost/*/mySpace?useLocalCache=true for local cache
- gigspaces.warmup - true/false value. Will perform a quick warm before starting the test.
- batchmode - true/false value. True will run benchmark in batch mode. false will run benchmark in single mode.
- batchsize - used when running in batch mode. This value represent the number of objects to write or read. 
- clearbeforetest - true/false value. true will clear the cache/space before starting the test. 
- payloadtype - Payload type: regular - will have a payload with a key and value field where value object exact number of items specified using the fieldcount property. Each item size determined using the fieldlength property. indexed - will have a payload with 5 fields. All indexed. 
- gemfireclientconfig - GemFire client config file. /export/home/shay/YCSB/gemfire/src/main/conf/client3.xml 
- payloadclasstype=used with Indexed payload mode. You can use any classes found under the com.yahoo.ycsb.db.gigaspaces.model package. 
- gemfireP2Pclient - true/false value. When true running GemFire in P2P mode. Should use relevant client config. When false running GemFire client in remote mode.
- multitest - true/false value. Used with embedded/P2P mode allowing load and read operations without restarting the JVM. When true main class should be com.yahoo.ycsb.MultiTestClient
- recordcount - Total Number of records to read/write
- operationcount - Total Number of operations
- workload=com.yahoo.ycsb.workloads.CoreWorkload
- readallfields - true/false value. 
- readproportion - Read operation proportion ratio. Use 1 when reading. Use 0 when writing.
- updateproportion - Update operation proportion ratio. 
- insertproportion - Insert operation proportion ratio. Use 0 when reading. Use 1 when writing.
- requestdistribution - zipfian
- fieldcount - Regular payload value number of fields.
- fieldlength - Regular payload value collection field size in bytes. 1024
- insertorder - Write oder mode. Support ordered mode and random mode.
- readorder - Read oder mode. Support ordered mode and random mode. 
- readrepeatcount - Repeat count for read operations
- collectlatencystats - true/false value. Control statistics collection. 
######################################################################

- Examples for benchmark command
single write 
for i in 1 2 3 4 5 6 7 8
do
	export ARG="$i"
	#single mode
	recordcount=$((ARG * 40000))
	operationcount=$((ARG * 40000))
	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P gs_workload -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true > "$OUTPUT_DIR/$ARG WRITE-Threads.txt" 2>&1
	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P gs_workload -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml > "$OUTPUT_DIR/$ARG READ-Threads.txt" 2>&1
done

single read
for i in 1 2 3 4 5 6 7 8
do
	export ARG="$i"
	#single mode
	recordcount=$((ARG * 40000))
	operationcount=$((ARG * 40000))
	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P gs_workload -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount -p clearbeforetest=true -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG WRITE-SINGLE_INDEXED-Threads.txt" 2>&1
done


single Indexed read
for i in 1 2 3 4 5 6 7 8
do
	export ARG="$i"
	#single mode
	recordcount=$((ARG * 40000))
	operationcount=$((ARG * 40000))
	$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P gs_workload -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index  > "$OUTPUT_DIR/$ARG READ-SINGLE_INDEXED-Threads.txt" 2>&1
done

batch write regular payload
$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P gs_workload -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true -p batchmode=true -p batchsize=100 -p payloadtype=regular > "$OUTPUT_DIR/$ARG Threads-WRITE-BATCH-REGUALR.txt" 2>&1

batch read regular payload
$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P gs_workload -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p batchmode=true -p batchsize=100 -p clearbeforetest=false -p payloadtype=regular -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml > "$OUTPUT_DIR/$ARG Threads-READ_BATCH-REGUALR.txt" 2>&1

batch write Indexed payload
$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -load -P gs_workload -threads $ARG -p collectlatencystats=false -p db=$CLIENT_DRIVER -p readrepeatcount=0 -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p recordcount=$recordcount -p operationcount=$operationcount  -p clearbeforetest=true -p batchmode=true -p batchsize=100 -p payloadtype=indexed -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG Threads-WRITE-BATCH-Indexed.txt" 2>&1

batch read Indexed payload
$JAVA_HOME/bin/java -classpath $GS_JARS:./core/target/core-0.1.4.jar:./core/target/classes $GC_ARGS com.yahoo.ycsb.Client -P gs_workload -threads $ARG -l "$ARG Thread"  -p collectlatencystats=false -p readrepeatcount=$readrepeatcount -p db=$CLIENT_DRIVER -p gigspaces.url=$gigspaces_url -p collectlatencystats=false -p recordcount=$recordcount -p operationcount=$operationcount -p gigspaces.warmup=false -p batchmode=true -p batchsize=100 -p clearbeforetest=false -p payloadtype=indexed -p gemfireclientconfig=/export/home/shay/YCSB/gemfire/src/main/conf/client3.xml -p payloadclasstype=com.yahoo.ycsb.db.gigaspaces.model.Class4Index > "$OUTPUT_DIR/$ARG Threads-READ_BATCH-Indexed.txt" 2>&1

How to analyze the results
######################################################################
The com.yahoo.ycsb.db.OutputAnalyzer takes as an argument a folder that includes benchmark results output files and generates an output that includes info about the "AverageLatency", "MinLatency", "MaxLatency","Throughput" for the "OVERALL" , "UPDATE", "READ", "INSERT" operations.
Here is an example for such an output:
OVERALL
1 WRITE-Threads.txt OVERALL Throughput sec), 1627.2068993572532
2 WRITE-Threads.txt OVERALL Throughput sec), 6305.170239596469
3 WRITE-Threads.txt OVERALL Throughput sec), 9824.791223186507
4 WRITE-Threads.txt OVERALL Throughput sec), 12341.869793273681
5 WRITE-Threads.txt OVERALL Throughput sec), 14850.01485001485
6 WRITE-Threads.txt OVERALL Throughput sec), 17253.774263120056
7 WRITE-Threads.txt OVERALL Throughput sec), 18990.775908844276
8 WRITE-Threads.txt OVERALL Throughput sec), 21217.34517968439

1 READ-Threads.txt OVERALL Throughput sec), 2115.730455939913
2 READ-Threads.txt OVERALL Throughput sec), 7139.032661074424
3 READ-Threads.txt OVERALL Throughput sec), 9764.035801464606
4 READ-Threads.txt OVERALL Throughput sec), 12892.828364222401
5 READ-Threads.txt OVERALL Throughput sec), 14682.131845543972
6 READ-Threads.txt OVERALL Throughput sec), 19505.85175552666
7 READ-Threads.txt OVERALL Throughput sec), 19107.410945816842
8 READ-Threads.txt OVERALL Throughput sec), 23647.65001477978
