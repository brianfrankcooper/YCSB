<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Ignite Driver for YCSB
This driver enables YCSB to work with Ignite

For more comprehensive Ignite testing please look at project Yardstick

https://github.com/gridgain/yardstick

## Getting Started
### 1. Start your Ignite cluster



### 2. Set up YCSB
You can clone the YCSB project and compile it to stay up to date with the latest changes. Or you can just download the latest release and unpack it. Either way, instructions for doing so can be found here: https://github.com/brianfrankcooper/YCSB.



## Properties

  * igniteIPs - comma separated list of discovery IPs for Ignite cluster
  * insertAsync true|false 
  * batchSize - batch size, if more than 1 then insert operations done in bulk 



## Load data

    bin/ycsb load ignite -P workloads/workloada

or load asynchronously

    bin/ycsb load ignite \
                      -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
                      -p insertAsync=true \
                      -p batchSize=10 \
                      -p threadcount=2 \
                      -p recordcount=100 \
                      -p operationcount=100
                      
                      
or load synchronously  

    bin/ycsb load ignite \
                      -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
                      -p insertAsync=false \
                      -p batchSize=10 \
                      -p threadcount=2 \
                      -p recordcount=100 \
                      -p operationcount=100                    

## Run

    bin/ycsb run ignite -P workloads/workloada



Check style
--
    mvn checkstyle:check  -Dcheckstyle.config.location=$(pwd)/../checkstyle.xml