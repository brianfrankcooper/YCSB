#!/bin/bash

# Get the IP and the record count from the user. The default value for the
# recordcount is 1 million and for the ip is '127.0.0.1'.
recordcount=${recordcount:-1000000}
ip=${ip:-127.0.0.1}
while [ $# -gt 0 ]; do
   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
   fi
  shift
done

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ycsb=$DIR/bin/ycsb
db_properties=$DIR/db.properties
cqlsh="cqlsh $ip"
params="-p recordcount=$recordcount -p operationcount=10000000"

create_table() {
  $cqlsh --keyspace ycsb --execute 'create table usertable (y_id varchar primary key, field0 varchar, field1 varchar, field2 varchar, field3 varchar, field4 varchar, field5 varchar, field6 varchar,  field7 varchar, field8 varchar, field9 varchar);'
}
delete_data() {
  $cqlsh --keyspace ycsb --execute 'drop table usertable;'
}
setup() {
  $cqlsh --execute 'create keyspace ycsb;'
}
cleanup() {
  $cqlsh --execute 'drop keyspace ycsb;'
}
run_workload() {
    local workload=$1
    create_table
    echo =========================== $workload ===========================
    $ycsb load yugabyteCQL -P workloads/$workload -P $db_properties $params \
      -p threadcount=32 -s > $workload-ycql-load.dat
    $ycsb run yugabyteCQL -P workloads/$workload -P $db_properties $params \
      -p threadcount=256 -p maxexecutiontime=180 -s > $workload-ycql-transaction.dat
    delete_data
}

setup
run_workload workloada
run_workload workloadb
run_workload workloadc
run_workload workloadd
run_workload workloade
run_workload workloadf
cleanup

