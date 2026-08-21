#!/bin/bash
#
# Copyright (c) 2012 - 2021 YCSB contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
set -e

__DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

declare -a HEADERS=(
    "AverageLatency(us)"
    "95thPercentileLatency(us)"
)

   # "outputLoad"
declare -a FILES_LIST=(
   "outputRun"
)

declare -a OPERATIONS=(
   "INSERT"
   "READ"
   "UPDATE"
   "READ-MODIFY-WRITE"
   "SCAN"
)

if [ -z "${DO_SET_DATA_DIR}" ]; then
    DATA_DIR=${__DIR}/../outputs/
    PLOT_DATA=${__DIR}/data
    PLOT_FILE=${__DIR}/data/data
else
    DATA_DIR=${DO_SET_DATA_DIR}/outputs/
    PLOT_DATA=${DO_SET_DATA_DIR}/data
    mkdir -p ${PLOT_DATA}
    rm -f ${PLOT_DATA}/*
    PLOT_FILE=${PLOT_DATA}/data
fi

# echo "DATA_DIR: $DATA_DIR"
# echo "PLOT_DATA: $PLOT_DATA"

# $1 operation
# $2 workload type
function extract_data_from_file() {
   local plot_file=${PLOT_FILE}_${2}.dat
   local op="${1}"
   # ===================================================================
   # check if there is something to write ...
   # ===================================================================
   local write_row="False"
   for f in ${FILES_LIST[@]}; do
      for d in ${drivers_test[@]}; do
          local out_data="${DATA_DIR}/${d}"
          # echo "out_data $out_data/$d/$f_$2"
          [ ! -e "${out_data}/${f}_${2}.txt" ] && continue
          [ -z "$(cat ${out_data}/${f}_${2}.txt | grep ${op})" ] && continue
          write_row="True" && continue 2
      done
   done
   [ "${write_row}" = "False" ] && return
   # ===================================================================
   # Write
   # ===================================================================
   echo "$op    " | tr '\n' ' ' >> ${plot_file}
   for f in ${FILES_LIST[@]}; do
      for d in ${drivers_test[@]}; do
          local out_data="${DATA_DIR}/${d}"
          # echo "out_data $out_data/$d/$f_$2"

          [ ! -e "${out_data}/${f}_${2}.txt" ] && for h in ${HEADERS[@]}; do echo -ne "0.0     " >> ${plot_file}; done && continue

          # [ -z "$(cat ${out_data}/${f}_${2}.txt | grep ${op})" ] && echo -ne "0.0   0.0" >> ${plot_file} && continue
          for h in ${HEADERS[@]}; do
              # echo "   * Extracing data for $d -> $h"
              # cat ${out_data}/$f.txt | grep "\[${op}\]" | grep "$h" | cut -d',' -f3
              [ -z "$(cat ${out_data}/${f}_${2}.txt | grep "\[${op}\]" | grep "$h" | cut -d',' -f3)" ] &&  echo -ne "0.0     " >> ${plot_file}
              cat ${out_data}/${f}_${2}.txt | grep "\[${op}\]" | grep "$h" | cut -d',' -f3 | tr '\n' ' ' >> ${plot_file}
          done
      done
   done
   echo "" >> ${plot_file}
}

# $1 new file
function write_header() {
   local plot_file=${PLOT_FILE}_${1}.dat
   echo -n "" > ${plot_file}
   echo "Operation     " | tr '\n' ' ' >> ${plot_file}
   for h in ${drivers_test[@]}; do
      echo "$h" | tr '\n' ' ' >> ${plot_file}
      echo "$h-95" | tr '\n' ' ' >> ${plot_file}
   done
   echo "" >> ${plot_file}

}

function plot_v2() {
   write_header "$1"
   for o in ${OPERATIONS[@]}; do
      extract_data_from_file "${o}" "$1"
      local plot_file=${PLOT_FILE}_${1}.dat
      sed -i 's/READ-MODIFY-WRITE/R-M-W/g' $plot_file
      sed -i 's/NaN/0.0/g' $plot_file
   done
}


# ====================================== #
# include <...>
source ${__DIR}/../configs/config.sh
# ====================================== #

echo "Writing data to ${PLOT_DATA}:"
mkdir -p ${PLOT_DATA}
for w in ${workloads[@]}; do
   echo "   * Collecting $w"
   plot_v2 "$w"
done
