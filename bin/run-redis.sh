#!/bin/bash

./bin/ycsb load redis -s -P workloads/workloada -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputLoad.txt

./bin/ycsb run redis -s -P workloads/workloada -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunA.txt
./bin/ycsb run redis -s -P workloads/workloadb -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunB.txt
./bin/ycsb run redis -s -P workloads/workloadc -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunC.txt
./bin/ycsb run redis -s -P workloads/workloadf -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunF.txt
./bin/ycsb run redis -s -P workloads/workloadd -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunD.txt

redis-cli flushall

./bin/ycsb load redis -s -P workloads/workloade -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputLoad.txt
./bin/ycsb run redis -s -P workloads/workloade -p "redis.host=127.0.0.1" -p "redis.port=6379" -p "threadcount=8" > outputRunE.txt
