<!--
Copyright (c) 2018 YCSB contributors.
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

## Azure Cosmos DB Quick Start

This section describes how to run YCSB on Azure Cosmos DB. 

For more information on Azure Cosmos DB see 
https://azure.microsoft.com/services/cosmos-db/.

### 1. Setup
This benchmark expects you to have pre-created the database "ycsb" and
collection "usertable" before running the commands. When
prompted for a Partition Key, use "id". For RUs, select a value you
want to benchmark.  [RUs are the measure of provisioned thoughput](https://docs.microsoft.com/azure/cosmos-db/request-units)
that Azure Cosmos DB defines. The higher the RUs, the more throughput you will
get. You can override the default database name with the 
azurecosmos.databaseName configuration value for side-by-side
benchmarking.

You must set the uri and the primaryKey in the azurecosmos.properties file in the commands below.
    $YCSB_HOME/bin/ycsb load azurecosmos -P workloads/workloada -P azurecosmos/conf/azurecosmos.properties
    $YCSB_HOME/bin/ycsb run azurecosmos -P workloads/workloada -P azurecosmos/conf/azurecosmos.properties

Optionally you can set the uri and primaryKey as follows:
    $YCSB_HOME/bin/ycsb load azurecosmos -P workloads/workloada -p azurecosmos.primaryKey=<key from the portal> -p azurecosmos.uri=<uri from the portal>

### 2. Cosmos DB Configuration Parameters

#### Required parameters

- azurecosmos.uri < uri string > :
    - Path to your Azure Cosmos DB account and can be obtained from the portal. It will look like the following:  https://<your account name>.documents.azure.com:443/

- azurecosmos.primaryKey < key string > :
    - Obtained from the portal.  The
	  primary key is used to allow both read & write operations.  If you are
	  doing read only workloads you can substitute the readonly key from the
	  portal.

#### Options parameters

- azurecosmos.databaseName < name string > :
    - Name of the database to use.
    - Default: ycsb

- azurecosmos.useUpsert (true | false):
	- Set to true to allow inserts to update existing documents.  If this is 
	  false and a document already exists the insert will fail.
    - Default: false

- azurecosmos.includeExceptionStackInLog (true | false):
	- Determines if the full stack should be included in the log when an error happens.
	- The default is false to reduce output size.
    - Default: false

- azurecosmos.userAgent < agent string >:
	- The value to be appended to the user-agent header.
	- In most cases, you should leave this as "azurecosmos-ycsb".
    - Default: "azurecosmos-ycsb"

- azurecosmos.useGateway (true | false):
	- Specify if connection mode should use gateway as opposed to direct. By default, direct mode will be used, as the performance is generally better.
    - Default: false

- azurecosmos.consistencyLevel (STRONG | BOUNDED_STALENESS | SESSION | CONSISTENT_PREFIX | EVENTUAL):
	- If not specified, session level will be used by default. 
	- Default: SESSION

- azurecosmos.maxRetryAttemptsOnThrottledRequests < integer >
    - Set the maximum number of retries in the case where the request fails due to rate limiting.
    - Default: uses default value of azurecosmos Java SDK

- azurecosmos.maxRetryWaitTimeInSeconds < integer >
    - Sets the maximum timeout to for retry in seconds.
    - Default: uses default value of azurecosmos Java SDK
	
- azurecosmos.gatewayMaxConnectionPoolSize < integer >
   - Set the value of the connection pool size in gateway mode.
   
- azurecosmos.directMaxConnectionsPerEndpoint < integer >
   - Set the value of the max connections per endpoint in direct mode.

- azurecosmos.gatewayIdleConnectionTimeoutInSeconds < integer >
   - Sets the value of the timeout in seconds for an idle connection in gateway mode. After that time, the connection will be automatically closed.
   - Default: uses default value of azurecosmos Java SDK

- azurecosmos.directIdleConnectionTimeoutInSeconds < integer >
   - Sets the value of the timeout in seconds for an idle connection in direct mode. After that time, the connection will be automatically closed.
   - Default: uses default value of azurecosmos Java SDK


- azurecosmos.maxDegreeOfParallelism < integer >
    - Sets the number of concurrent operations run client side during parallel query execution.
    - Default: -1
	
- azurecosmos.maxBufferedItemCount < integer >
    - Sets the maximum number of items that can be buffered client side during parallel query execution.
    - Default: 0
	
- azurecosmos.preferredPageSize < integer >
    - Sets the preferred page size when scanning.
    - Default: -1


These parameters are also defined in a template configuration file in the
following location:
  $YCSB_HOME/azurecosmos/conf/azurecosmos.properties

### 3. FAQs

### 4. Example command
./bin/ycsb run azurecosmos -P workloads/workloadc -p azurecosmos.primaryKey=<your key eg:45fgt...==> -p azurecosmos.uri=https://<your account>.documents.azure.com:443/ -p recordcount=100 -p operationcount=100
