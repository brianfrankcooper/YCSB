<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2015 YCSB contributors.
All rights reserved.

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

# DynamoDB Binding

http://aws.amazon.com/documentation/dynamodb/

## Configure

    YCSB_HOME - YCSB home directory
    DYNAMODB_HOME - Amazon DynamoDB package files

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Using-the-Database-Libraries
for more information on setup.

# Benchmark

    $YCSB_HOME/bin/ycsb load dynamodb -P workloads/workloada -P dynamodb.properties
    $YCSB_HOME/bin/ycsb run dynamodb -P workloads/workloada -P dynamodb.properties

# Properties

    $DYNAMODB_HOME/conf/dynamodb.properties
    $DYNAMODB_HOME/conf/AWSCredentials.properties

# FAQs
* Why is the recommended workload distribution set to 'uniform'?
    This is to conform with the best practices for using DynamoDB - uniform,
evenly distributed workload is the recommended pattern for scaling and
getting predictable performance out of DynamoDB

For more information refer to
http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/BestPractices.html

* How does workload size affect provisioned throughput?
    The default payload size requires double the provisioned throughput to execute
the workload. This translates to double the provisioned throughput cost for testing.
The default item size in YCSB are 1000 bytes plus metadata overhead, which makes the
item exceed 1024 bytes. DynamoDB charges one capacity unit per 1024 bytes for read
or writes. An item that is greater than 1024 bytes but less than or equal to 2048 bytes
would cost 2 capacity units. With the change in payload size, each request would cost
1 capacity unit as opposed to 2, saving the cost of running the benchmark.

For more information refer to
http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/WorkingWithDDTables.html

* How do you know if DynamoDB throttling is affecting benchmarking?
    Monitor CloudWatch for ThrottledRequests and if ThrottledRequests is greater
than zero, either increase the DynamoDB table provisioned throughput or reduce
YCSB throughput by reducing YCSB target throughput, adjusting the number of YCSB
client threads, or combination of both.

For more information please refer to
https://github.com/brianfrankcooper/YCSB/blob/master/doc/tipsfaq.html

When requests are throttled, latency measurements by YCSB can increase.

Please refer to http://aws.amazon.com/dynamodb/faqs/ for more information.

Please refer to Amazon DynamoDB docs here:
http://aws.amazon.com/documentation/dynamodb/
