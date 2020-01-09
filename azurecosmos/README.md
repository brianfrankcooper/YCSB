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

## Azure Cosmos Quick Start

This section describes how to run YCSB on Azure Cosmos. 

For more information on Azure Cosmos see 
https://azure.microsoft.com/services/cosmos-db/

### 1. Setup
This benchmark expects you to have pre-created the database "ycsb" and
collection "usertable" before running the benchmark commands.  When
prompted for a Partition Key use id and for RUs select a value you
want to benchmark.  [RUs are the measure of provisioned thoughput](https://docs.microsoft.com/azure/cosmos-db/request-units)
that Azure Cosmos defines. The higher the RUs the more throughput you will
get. You can override the default database name with the 
azurecosmos.databaseName configuration value for side-by-side
benchmarking.

You must set the uri and the primaryKey in the azurecosmos.properties file in the commands below.
    $YCSB_HOME/bin/ycsb load azurecosmos -P workloads/workloada -P azurecosmos/conf/azurecosmos.properties
    $YCSB_HOME/bin/ycsb run azurecosmos -P workloads/workloada -P azurecosmos/conf/azurecosmos.properties

Optionally you can set the uri and primaryKey as follows:
    $YCSB_HOME/bin/ycsb load azurecosmos -P workloads/workloada -p azurecosmos.primaryKey=<key from the portal> -p azurecosmos.uri=<uri from the portal>

### 2. DocumenDB Configuration Parameters

#### Required parameters

- azurecosmos.uri < uri string > :
    - Obtained from the portal and gives a path to your azurecosmos database
	  account.  It will look like the following:  
	  https://<your account name>.documents.azure.com:443/

- azurecosmos.primaryKey < key string > :
    - Obtained from the portal and is the key to use for benchmarking.  The
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

- azurecosmos.connectionMode (DirectHttps | Gateway):
	- Some java operations only work when connecting via the gateway.  However
	  the best performance for basic operations like those used by YCSB are
	  obtained by using direct more where the client will connect directly to the
	  master server thats is managing the database and collection.
    - Default: DirectHttps

- azurecosmos.consistencyLevel (Strong | BoundedStaleness | Session | Eventual):
	- This setting defined the level on consistency you want for reads/scans
	  following inserts/updates. 
	- Default: Session

- azurecosmos.maxRetryAttemptsOnThrottledRequests < integer >
    - Sets the maximum number of retry attempts for throttled requests
    - Default: uses default value of azurecosmos Java SDK

- azurecosmos.maxRetryWaitTimeInSeconds < integer >
    - Sets the maximum timeout to for retry in seconds
    - Default: uses default value of azurecosmos Java SDK

- azurecosmos.maxDegreeOfParallelismForQuery < integer >
    - Sets the maximum degree of parallelism for the FeedOptions used in Query operation
    - Default: 0

- azurecosmos.includeExceptionStackInLog (true | false):
    - Determines if the full stack when and error happens should be included in the log.
	  The default is false to reduce a lot of log spew.

- azurecosmos.maxConnectionPoolSize < integer >
   - This is the number of connections maintained for operations.
   - See the JAVA SDK documentation for ConnectionPolicy.getMaxPoolSize

- azurecosmos.idleConnectionTimeout < integer >
   - This value is in seconds and determines how quickly a connection is recycled.
   - See the JAVA SDK documentation for ConnectionPolicy.setIdleConnectionTimeout.

These parameters are also defined in a template configuration file in the
following location:
  $YCSB_HOME/azurecosmos/conf/azurecosmos.properties

### 3. FAQs

### 4. Example command
./bin/ycsb run azurecosmos -s -P workloads/workloadb -p azurecosmos.primaryKey=<your key eg:45fgt...==> -p azurecosmos.uri=https://<your account>.documents.azure.com:443/ -p recordcount=100 -p operationcount=100
