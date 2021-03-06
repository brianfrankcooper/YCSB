<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
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

YCSB
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)



Links
-----
* To get here, use https://ycsb.site
* [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

Getting Started
---------------

1. Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

    ```sh
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
    tar xfvz ycsb-0.17.0.tar.gz
    cd ycsb-0.17.0
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 

    On Linux:
    ```sh
    bin/ycsb.sh load basic -P workloads/workloada
    bin/ycsb.sh run basic -P workloads/workloada
    ```

    On Windows:
    ```bat
    bin/ycsb.bat load basic -P workloads\workloada
    bin/ycsb.bat run basic -P workloads\workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.


Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package

Running multiple instances and latency percentiles
--------------------------------------------------

In general, you shall be interested in 99% percentile (P99) of the latency
distribution, and the rest of the tail - 99.9%, 99.99%, 99.999%. The difference
between the amount of requests that will be observed by a user that fall
into 95% (P95) percentile and 99% percentile may be sufficiently large.

For example, see "How Many Nines?" at https://bravenewgeek.com/everything-you-know-about-latency-is-wrong/.
The formula to calculate probability of how many clients will observe
a specific percentile is: 

    Probability_to_observe = 1 - Percentile ^ Requests

That is why almost 30% of the users will observe latency worse than P99
just by loading the default _google.com_ web page:

    1 - 0.99 ^ 30 = 0.27

Remember, that

- _latencies_ percentiles can't be averaged. Don't fall into this
  [trap](http://latencytipoftheday.blogspot.com/2014/06/latencytipoftheday-you-cant-average.html).
  Neither latency averages, nor P99 averages do not make any sense.

If you run multiple loaders dump result histograms with:

    -p hdrhistogram.fileoutput=true
    -p hdrhistogram.output.path=file.hdr

merge them manually and extract required percentiles out of the
joined result.

Remember that running multiple workloads may distort original
workloads distributions they were intended to produce.

Merging HDR histogram percentiles
---------------------------------

HdrHistogram can serialize its data to HDR files. Use CLI tool
to do different operations with your saved histograms
https://github.com/nitsanw/HdrLogProcessing.

You shall be interested in 3 functions:

- Union - to combine result histograms
- Summarize - to extract latency percentiles
- An ability to print the result into the CSV file and extract tags

To extract HDR content into CSV file format use from
https://github.com/HdrHistogram/HdrHistogram/:

    java -cp HdrHistogram-2.1.9.jar org.HdrHistogram.HistogramLogProcessor -i file.hdr -o output_${tag}.csv -csv -tag ${tag}
