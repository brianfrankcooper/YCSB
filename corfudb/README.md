## CorfuDB Quick Start


The YCSB workload generator takes a workload specification (i.e, workloads/workloada) and starts generating read/writes
against a single table, specifically, a table named "usertable". By default, each worker thread uses a separate DB
instance. In certain cases we would like to benchmark the performance of multiple threads within a single client DB
instance. The CorfuDB driver implements two modes, single and multiple, these two modes allows for benchmarking CorfuTable's
performance within a single CorfuRuntime and across multiple runtimes. The reads are implemented as non-transaction, and
the writes are transactional and are retried up to 3 times. Finally, the driver will also start a compaction task
that will checkpoint the "usertable" stream and trim the log every 2 minutes. 


## Example Command

The command below will run the load generator against a single node cluster listening at localhost:9000

Options:

`-s` emit status log periodically

`-P` workload specification

`-p corfu.connection` CorfuDB cluster endpoint

`-p corfu.singleclient="true" ` Share's a single client between all worker threads

`-p hdrhistogram.percentiles=50,90,95,99` collect stats for different percentiles

`-p exportfile=stats.json` Creates a cummulative histogram at the end of the run and writes it to stats.json




```
time $YCSB_DIR run corfudb -s -P workloads/workloada -p corfu.connection="localhost:9000" -p corfu.singleclient="true" -p hdrhistogram.percentiles=50,90,95,99 -p exporter=site.ycsb.measurements.exporter.JSONMeasurementsExporter -p exportfile=stats.json

```
