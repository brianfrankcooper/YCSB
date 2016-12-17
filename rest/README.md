<!--
Copyright (c) 2016 YCSB contributors. All rights reserved.

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

## Quick Start

This section describes how to run YCSB to benchmark HTTP RESTful
webservices. The aim of the rest binding is to benchmark the 
performance of any sepecific HTTP RESTful webservices with real
life (production) dataset. This must not be confused with benchmarking
various webservers (like Apache Tomcat, Nginx, Jetty) using a dummy 
dataset.

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:rest-binding -am clean package

### 2. Set Up an HTTP Web Service

There must be a running HTTP RESTful webservice accesible from 
the instance on which YCSB is running. If the webservice is 
running on the local instance default HTTP port 80, it's base
URL will look like http://127.0.0.1:80/{service_endpoint}. The
rest binding assumes that the webservice to be benchmarked already
has a valid dataset. THe rest module has been designed in this 
way for two reasons:

1. The performance of most webservices depends on the size, pattern
and the nature of the real life dataset accesible from these services.
Hence creating a dummy dataset might not actually reflect the true
performance of a webservice to be benchmarked.

2. Since many webservices have a non-naive backend which includes
interaction with multiple backend components, tables and databases.
Generating a dummy dataset for such webservices is a non-trivial and
a time consuming task.

However to benchmark a webservice before it has access to a real 
dataset, support for automatic data insertion can be added in the
future. An example of such a scenario is benchmarking a webservice
before it moves to production.

### 3. Run YCSB
    
At this point we assume that you've setup a webservice accesible at
an HTTP endpoint like this: http://{host}:{port}/{service_endpoint}.

Before you are ready to run please ensure that you have prepared a
trace for the CRUD operations to benchmark your webservice. 

Trace is a collection of URL resources that should be hit in order
to benchmark any webservice. The more realistic this collection of
URL is, the more reliable and accurate are the benchmarking results
because this means simulating the real life workload more accurately.
Tracefile is a file that holds the trace. For example, if your 
webservice exists at http://{host}:{port}/{endpoint}, and you want
to benchmark the performance of READS on this webservice with five
resources (namely resource_1, resource_2 ... resource_5) then the
url.trace.read file will look like this:

http://{host}:{port}/{endpoint}/resource_1
http://{host}:{port}/{endpoint}/resource_2
http://{host}:{port}/{endpoint}/resource_3
http://{host}:{port}/{endpoint}/resource_4
http://{host}:{port}/{endpoint}/resource_5

The rest module will pick up URLs from the above file according to
the `requestdistribution` property (default is zipfian) mentioned in
the rest_workload. In the example above we assume that the property 
`url.prefix` (see below for property description) is set to empty. If
url.prefix property is set to `http://{host}:{port}/{endpoint}/` the 
equivalent of the read trace given above would look like:

resource_1
resource_2
resource_3
resource_4
resource_5

In real life the traces for various CRUD operations are diffent
from one another. HTTP GET will rarely have the same URL access
pattern as that of HTTP POST or HTTP PUT. Hence to give enough
flexibility to benchmark webservices, different trace files can
be used for different CRUD operations. However if you wish to use
the same trace for all these operations, just pass the same file
to all these properties - `url.trace.read`, `url.trace.insert`,
`url.trace.update` & `url.trace.delete`.

Now you are ready to run! Run the rest_workload:

    ./bin/ycsb run rest -s -P workloads/rest_workload

For further configuration see below: 

### Default Configuration Parameters
The default settings for the rest binding are as follows:

- `url.prefix` 
  - The base endpoint URL where the webservice is running. URLs from trace files (DELETE, GET, POST, PUT) will be prefixed with this value before making an HTTP request. A common usage value would be http://127.0.0.1:8080/{yourService}
  - Default value is `http://127.0.0.1:80/`.
  
- `url.trace.read` 
  - The path to a trace file that holds the URLs to be invoked for HTTP GET method. URLs must be seperated by a newline.
  
- `url.trace.insert` 
  - The path to a trace file that holds the URLs to be invoked for HTTP POST method. URLs must be seperated by a newline. 

- `url.trace.update` 
  - The path to a trace file that holds the URLs to be invoked for HTTP PUT method. URLs must be seperated by a newline.

- `url.trace.delete` 
  - The path to a trace file that holds the URLs to be invoked for HTTP DELETE method. URLs must be seperated by a newline.

- `headers` 
  - The HTTP request headers used for all requests. Headers must be separated by space as a delimiter.
  - Default value is `Accept */* Accept-Language en-US,en;q=0.5 Content-Type application/x-www-form-urlencoded user-agent Mozilla/5.0`

- `timeout.con` 
  - The HTTP connection timeout in seconds. The response will be considered as an error if the client fails to connect with the server within this time limit.
  - Default value is `10` seconds.

- `timeout.read` 
  - The HTTP read timeout in seconds. The response will be considered as an error if the client fails to read from the server within this time limit.
  - Default value is `10` seconds.
  
- `timeout.exec` 
  - The time within which request must return a response. The response will be considered as an error if the client fails to complete the request within this time limit.
  - Default value is `10` seconds.

- `log.enable` 
  - A Boolean value to enable console status logs. When true, it will print all the HTTP requests being made and thier response status on the YCSB console window.
  - Default value is `false`.

- `readrecordcount` 
  - An integer value that signifies the top k URLs (entries) to be picked from the `url.trace.read` file for making HTTP GET requests. Must have a value greater than 0. If this value exceeds the number of entries present in `url.trace.read` file, then k will be set to the number of entries in the file.
  - Default value is `10000`. 

- `insertrecordcount` 
  - An integer value that signifies the top k URLs to be picked from the `url.trace.insert` file for making HTTP POST requests. Must have a value greater than 0. If this value exceeds the number of entries present in `url.trace.insert` file, then k will be set to the number of entries in the file.
  - Default value is `5000`. 

- `deleterecordcount` 
  - An integer value that signifies the top k URLs to be picked from the `url.trace.delete` file for making HTTP DELETE requests. Must have a value greater than 0. If this value exceeds the number of entries present in `url.trace.delete` file, then k will be set to the number of entries in the file.
  - Default value is `1000`.

- `updaterecordcount` 
  - An integer value that signifies the top k URLs to be picked from the `url.trace.update` file for making HTTP PUT requests. Must have a value greater than 0. If this value exceeds the number of entries present in `url.trace.update` file, then k will be set to the number of entries in the file.
  - Default value is `1000`.

- `readzipfconstant` 
  - An double value of the Zipf's constant to be used for insert requests. Applicable only if the requestdistribution = `zipfian`.
  - Default value is `0.9`.

- `insertzipfconstant` 
  - An double value of the Zipf's constant to be used for insert requests. Applicable only if the requestdistribution = `zipfian`. 
  - Default value is `0.9`.
  
- `updatezipfconstant` 
  - An double value of the Zipf's constant to be used for insert requests. Applicable only if the requestdistribution = `zipfian`. 
  - Default value is `0.9`.

- `deletezipfconstant` 
  - An double value of the Zipf's constant to be used for insert requests. Applicable only if the requestdistribution = `zipfian`. 
  - Default value is `0.9`.
