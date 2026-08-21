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
__PLOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
__ORIGINAL_PLOT_DIR=${__PLOT_DIR}


[ ! -z "${DO_SET_DATA_DIR}" ] && __PLOT_DIR=${DO_SET_DATA_DIR}

source ${__ORIGINAL_PLOT_DIR}/../configs/config.sh
cd ${__PLOT_DIR}
echo "[INFO] Now in directory ${PWD}"
mkdir -p ${__PLOT_DIR}/images


# This function modify the global variable gnuplot_command!!!
# $1 nam of the driver
function append_title_for_driver() {
   local a="${1}"
   case "${a}" in
      orientdb*)
          name=$(echo ${a} | sed 's/orientdb/ODB/')
          gnuplot_command+="'$name'"
          ;; 
      *)
          gnuplot_command+="'$d'"
   esac

}

# $1 filename
function generate_gnuplot_single() { 
    local file_data="${1}"
    gnuplot_command=""
    read -d '' gnuplot_command << \
_______________________________________________________________________________ || true
set title plot_titlel;
set auto x;
set logscale y; 
set yrange [1:*];
set style data histogram;
set style histogram cluster gap 1;
set style fill solid border -1;
set boxwidth 0.8;
set key left top;
set grid;
set ylabel "time pro requet (us)";
_______________________________________________________________________________
    local i=3
    gnuplot_command+=" plot filename using $i:xtic(1) ti "
    for d in ${drivers_test[@]}; do
        [ $i -gt 3 ] && gnuplot_command+=", '' u $i ti "
        append_title_for_driver "$d"
        i=$((i+2))
    done
    
    
}

# $1 filename
function generate_gnuplot_multi() { 

    local first=$(echo "sqrt(${#workloads[@]})" | bc)
    local second=$((${#workloads[@]}/$first))
    [ $(($first * $second)) -lt ${#workloads[@]}  ] && second=$((second+1))
    local file_data="${1}"
    gnuplot_command=""
    local title="YCSB"
    for d in ${drivers_test[@]}; do
        title+=" - $d"
    done
    read -d '' gnuplot_command << \
_______________________________________________________________________________ || true
set multiplot layout ${first}, ${second} title '$title';
set auto x;
set yrange [1:*];
set style data histogram;
set style histogram cluster gap 1;
set style fill solid border -1;
set boxwidth 0.8;
set xtic rotate by -45 scale 0;
set key left top;
set grid;
set logscale y;
set key samplen 1;
set key spacing 1;
_______________________________________________________________________________

    for w in ${workloads[@]}; do
       local i=3
       gnuplot_command+=";set ylabel '$w - time (us)';"
       gnuplot_command+=" plot 'data/data_${w}.dat' using $i:xtic(1) ti "
       for d in ${drivers_test[@]}; do
           [ $i -gt 3 ] && gnuplot_command+=", '' u $i ti "
           append_title_for_driver "$d"
           i=$((i+2))
       done
    done
    
    
}

# ========================================================================================= #
if [[ "$1" = "serie" ]]; then
# ========================================================================================= #
   for w in ${workloads[@]}; do
      echo "plotting $w"
      # gnuplot -e "filename='data/data_${w}.dat'; plot_titlel='YCSB-workload $w'" "${__PLOT_DIR}/plot_ycsb.gn"
      generate_gnuplot_single "data/data_${w}.dat"
      gnuplot -e "filename='data/data_${w}.dat'; plot_titlel='YCSB-workload $w'; ${gnuplot_command}; pause(-1)"
   done
# ========================================================================================= #
elif [[ "$1" = "save" ]]; then
# ========================================================================================= #
   for w in ${workloads[@]}; do
      echo "Saving plot $w"
      generate_gnuplot_single "data/data_${w}.dat"
      echo | gnuplot -e "set terminal png size 800,600; set output 'images/workload_$w.png';filename='data/data_${w}.dat'; plot_titlel='YCSB-workload $w'; ${gnuplot_command}"
   done
   echo "Saving multi-plot"
   generate_gnuplot_multi
   gnuplot -e "set terminal png size 1600,1200; set output 'images/workload_all.png'; $gnuplot_command"
# ========================================================================================= #
elif [[ "$1" = "multi" ]]; then
# ========================================================================================= #
      echo "multiplot"
      generate_gnuplot_multi
      echo "$gnuplot_command"
      gnuplot -e "$gnuplot_command; pause(-1)"
# ========================================================================================= #
elif [[ "$1" = "n" ]]; then
# ========================================================================================= #
      echo "plot $2"
      generate_gnuplot_single "data/data_${w}.dat"
      gnuplot -e "filename='data/data_${2}.dat'; plot_titlel='YCSB-workload ${2}'; ${gnuplot_command}; pause(-1)"
else
   echo "$0 [serie|multi|save|n] <n.plot>"
fi

# save plot name ...
# for d in $(ls | grep 'test_2020'); do new_name=$(echo "$d" | sed 's#test_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9]-[0-9][0-9]-[0-9][0-9]##'); echo "${new_name}"; cp $d/images/workload_all.png all_tests/${new_name}.png; done
