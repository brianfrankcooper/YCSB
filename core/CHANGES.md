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

When used as a latency under load benchmark YCSB in it's original form suffers from
Coordinated Omission[1] and related measurement issue:

* Load is controlled by response time
* Measurement does not account for missing time
* Measurement starts at beginning of request rather than at intended beginning
* Measurement is limited in scope as the histogram does not provide data on overflow values

To provide a minimal correction patch the following were implemented:

1. Replace internal histogram implementation with HdrHistogram[2]:
HdrHistogram offers a dynamic range of measurement at a given precision and will
improve the fidelity of reporting. It allows capturing a much wider range of latencies.
HdrHistogram also supports compressed loss-less serialization which enable capturing
snapshot histograms from which lower resolution histograms can be constructed for plotting
latency over time. Snapshot interval histograms are serialized on status reporting which
must be enabled using the '-s' option.
 
2. Track intended operation start and report latencies from that point in time:
Assuming the benchmark sets a target schedule of execution in which every operation
is supposed to happen at a given time the benchmark should measure the latency between
intended start time and operation completion.
This required the introduction of a new measurement point and inevitably
includes measuring some of the internal preparation steps of the load generator.
These overhead should be negligible in the context of a network hop, but could
be corrected for by estimating the load-generator overheads (e.g. by measuring a
no-op DB or by measuring the setup time for an operation and deducting that from total).
This intended measurement point is only used when there is a target load (specified by
the -target paramaeter)

This branch supports the following new options:

* -p measurementtype=[histogram|hdrhistogram|hdrhistogram+histogram|timeseries] (default=histogram)
The new measurement types are hdrhistogram and hdrhistogram+histogram. Default is still
histogram, which is the old histogram. Ultimately we would remove the old measurement types
and use only HdrHistogram but the old measurement is left in there for comparison sake.

* -p measurement.interval=[op|intended|both] (default=op)
This new option deferentiates between measured intervals and adds the intended interval(as described)
above, and the option to record both the op and intended for comparison.

* -p hdrhistogram.fileoutput=[true|false] (default=false)
This new option will enable periodical writes of the interval histogram into an output file. The path can be set using '-p hdrhistogram.output.path=<PATH>'.

When running the benchmark with graph workloads following parameters are available:

* -p datasetdirectory=[/path/on/your/machine] (default=pathToYCSB/benchmarkingData)
This new option will store the values created during a `GraphWorkload` execution in the given folder. For later runs 
this data will be used to mimic that workload run, removing randomness.

* -p nodebytesize=[an integer value] (default=500)
This new option will set the size (in bytes) of the value (a string) stored in each node. This will determine how 
much in terms of data size will be stored in the data base.

These two parameters alter the graph structure a little bit: (See com.yahoo.ycsb.generator.graph.GraphDataRecorder to
 see the generation process)

* -p testparametercount=[an integer value] (default=128)
This new option will set the number of tests per product in the `GraphWorkload`, more specifically 
in the `GraphDataRecorder`.

* -p productsperorder=[an integer value] (default=10)
This new option will set the number of products in one order.

Example parameters:
-target 1000 -s -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p basicdb.verbose=false -p basicdb.simulatedelay=4 -p measurement.interval=both -p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=true -p maxexecutiontime=60

Further changes made:

* -p status.interval=<number of seconds> (default=10)
Controls the number of seconds between status reports and therefore between HdrHistogram snapshots reported.

* -p basicdb.randomizedelay=[true|false] (default=true)
Controls weather the delay simulated by the mock DB is uniformly random or not.

Further suggestions:

1. Correction load control: currently after a pause the load generator will do
operations back to back to catchup, this leads to a flat out throughput mode
of testing as opposed to controlled load.

2. Move to async model: Scenarios where Ops have no dependency could delegate the
Op execution to a threadpool and thus separate the request rate control from the
synchronous execution of Ops. Measurement would start on queuing for execution.

1. https://groups.google.com/forum/#!msg/mechanical-sympathy/icNZJejUHfE/BfDekfBEs_sJ
2. https://github.com/HdrHistogram/HdrHistogram