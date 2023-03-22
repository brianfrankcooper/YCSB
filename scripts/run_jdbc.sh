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
ysqlsh="ysqlsh -h $ip"
params="-p recordcount=$recordcount -p operationcount=10000000"

create_table() {
        $ysqlsh -d ycsb -c "CREATE TABLE usertable (YCSB_KEY TEXT, FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT, PRIMARY KEY (YCSB_KEY ASC)) SPLIT AT VALUES(('user10'),('user14'),('user18'),('user22'),('user26'),('user30'),('user34'),('user38'),('user42'),('user46'),('user50'),('user54'),('user58'),('user62'),('user66'),('user70'),('user74'),('user78'),('user82'),('user86'),('user90'),('user94'),('user98'));"
}
delete_data() {
  $ysqlsh -d ycsb -c 'drop table usertable;'
}
setup() {
  $ysqlsh -c 'create database ycsb;'
}
cleanup() {
  $ysqlsh -c 'drop database ycsb;'
}

run_workload() {
    local workload=$1
    create_table
    echo =========================== $workload ===========================
    $ycsb load jdbc -P workloads/$workload -P $db_properties $params \
      -p threadcount=32 -s > $workload-ysql-load.dat
    $ycsb run jdbc -P workloads/$workload -P $db_properties $params \
      -p threadcount=256 -p maxexecutiontime=180 -s > $workload-ysql-transaction.dat
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
