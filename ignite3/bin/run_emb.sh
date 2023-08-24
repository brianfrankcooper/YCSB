#!/usr/bin/env bash

# Use this script to run benchmarks with embedded AI3 node
# Has to be run from the "target/ycsb-ignite3-binding-*" directory

set -x

declare -a BINDINGS=(
  "ignite3"
  "ignite3-sql"
  "ignite3-jdbc"
)

declare -a WORKLOADS=(
  "./workloads/workloadc"
  "../../workloads/embedded_node"
)

GLOBAL_ARGS=
#GLOBAL_ARGS="-p operationcount=10 -p recordcount=10 -p debug=true"

function runYscb() {
  local mode=$1
  local binding=$2
  local logFileName=$3
  local addArgs=$4

  echo ">>> Running YCSB for ${binding}, mode: ${mode}, args: ${addArgs}"

  local workloads=
  for wl in "${WORKLOADS[@]}"; do
    workloads="${workloads} -P ${wl}"
  done

  bin/ycsb.sh "${mode}" "${binding}" \
      -s \
      "${workloads}" \
      "${GLOBAL_ARGS}" \
      "${addArgs}" \
      2>&1 | tee -a "${logFileName}"

  # Save YCSB results in a separate file
  grep -E -e "^\[[A-Z]" -e "Command line" "${logFileName}" > "${logFileName}.res.csv"
}

function runBinding() {
  local binding=$1

  echo ">> Running YCSB for ${binding}"

  timestamp=$(date +%F-%H-%M-%S)
  logFileName="../../${timestamp}-${binding}.log"

  rm -rf ignite3-work

  runYscb "load" "${binding}" "${logFileName}" "-threads 4"
  runYscb "run"  "${binding}" "${logFileName}" "-threads 1"
  runYscb "run"  "${binding}" "${logFileName}" "-threads 4"
}

for binding in "${BINDINGS[@]}"; do
  runBinding "${binding}"
done
