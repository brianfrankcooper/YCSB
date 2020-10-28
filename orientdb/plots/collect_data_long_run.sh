#!/bin/bash

set -e

__DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEADER="95thPercentileLatency(us)"

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

# __DO_DEBUGGING="True"

function _debug() {
    [ -z "${__DO_DEBUGGING}" ] && return
    for o in "$@"; do
        echo -ne "[DBG] "
        echo "$o "
    done
}


# declare -A THREADS_TO_PLOT=()

function collect_number_threads() {
   i=0
   local __tmp_array=()
   for dir in $(ls "${BASE_DIRECTORY}"/ | grep "test_" | grep "recordcount_${COUNTRECORDS}_" | grep "batchsize_${BATCHSIZE}_"); do
      base_name_dir="$(basename ${dir})"
      threads_num="$(echo $base_name_dir | sed 's/.*threads_\([0-9]*\)_/\1/' )"
      _debug "  * visiting dir -> $dir" \
             "      -> $base_name_dir"  \
             "      -> threads $threads_num"
      __tmp_array[$i]="${threads_num}"
      i=$((i+1))
   done
   # readarray -t sorted < <(printf '%s\0' "${THREADS_TO_PLOT[@]}" | sort -z | xargs -0n1)
   THREADS_TO_PLOT=($(printf '%s\n' "${__tmp_array[@]}"|sort -n))
   _debug "Threads collected"
   _debug "${THREADS_TO_PLOT[@]}"
}


# $1 directory
# $2 workload
# $3 operation
function collect_data_for_workload_and_operation() {
    local _dir="${1}"
    local _workload="${2}"
    local _operation="${3}"
    # _debug "  * $(basename ${_dir}); $_workload; $_operation"
    for f in "${FILES_LIST[@]}"; do
        local _file_to_read="${_dir}/${f}_${_workload}.txt"
        _debug "    * Reading file for $(basename ${_dir})"
        [ ! -e ${_file_to_read} ] && _debug "      !!!!! data for driver do not exist !!!!!!" && echo -ne " 0.0 " >> ${OUT_FILE} && continue
        _debug "      ** data for driver exists!"
        [ -z "$(grep "\[${_operation}\]" ${_file_to_read} | grep "${HEADER}" )" ] && echo -ne " 0.0 " >> ${OUT_FILE}
        echo -ne " $(grep "\[${_operation}\]" ${_file_to_read} | grep "${HEADER}" | cut -d',' -f3 | tr '\n' ' ') " >>  ${OUT_FILE}
    done

}

# $1 thread number
# $2 workload
# $3 operations
function collect_data_drivers_for_thread() {
   local thread="${1:-0}"
   _debug "Thread $thread "
   echo -ne "$thread " >> ${OUT_FILE}
   # ===============================
   # collect data from directories:
   for dir in $(ls "${BASE_DIRECTORY}"/ | grep "test_" | grep "recordcount_${COUNTRECORDS}_" | grep "batchsize_${BATCHSIZE}_" | grep "threads_${thread}"); do
       for h in ${drivers_test[@]}; do
           # [ ! -e "${BASE_DIRECTORY}/${dir}/outputs/${h}" ] && return
           _debug "   : visiting driver $h"
           collect_data_for_workload_and_operation "${BASE_DIRECTORY}/${dir}/outputs/${h}" "${2}" "${3}"
       done
   done
   # ===============================
   echo >> ${OUT_FILE}
}

# $1 file
function write_header() {
   local _file="${1}"
   echo -ne "threads " > ${_file} 
   for h in ${drivers_test[@]}; do
       echo -ne " $h " >> ${_file}
   done
   echo >> ${_file}
}

function collect_data_for_workload() {
    _workload=${1}
    local i=0
    n_operations="$(($#-1))"
    echo "Plotting for $n_operations operations" 
    
    read -d '' gnuplot_command << \
_______________________________________________________________________________ || true
set logscale y; 
set xlabel 'threads';
set style data linespoints; 
set grid; 
_______________________________________________________________________________

    [ $n_operations -gt 1 ] && gnuplot_command+="; set multiplot layout ${n_operations}, 1 title 'YCSB-${_workload}-${_operation}_recordcount_${COUNTRECORDS}-batchsize_${BATCHSIZE}'"
    [ $n_operations -eq 1 ] && gnuplot_command+="set title 'YCSB-${_workload}-${_operation}_recordcount_${COUNTRECORDS}-batchsize_${BATCHSIZE}'"

    for _operation in "${@}"; do
       [ $i -eq 0 ] && i=1 && continue
       echo "Collecting data for ${_workload}-${_operation}"
       OUT_FILE="${OUT_DIR}/plot_${_workload}${_operation}.dat"
       write_header "${OUT_FILE}"
       for t in "${THREADS_TO_PLOT[@]}"; do
           collect_data_drivers_for_thread "$t" "${_workload}" "${_operation}"
       done
    # gnuplot_command="set logscale y; set xlabel 'threads'; set ylabel 'time(us)'; set style data linespoints; set grid; set title 'YCSB-${_workload}-${_operation}_recordcount_${COUNTRECORDS}-batchsize_${BATCHSIZE}'; plot '${OUT_FILE}' u 1:2 w linespoints title columnhead"
       gnuplot_command+=";set ylabel '${_operation} time(us)' ; plot '${OUT_FILE}' u 1:2 w linespoints title columnhead"
    # echo "$gnuplot_command"
    local j=2
    for h in ${drivers_test[@]}; do
       [ $j -eq 2 ] && j=3 && continue
        gnuplot_command+=", '' u 1:$j w linespoints title columnhead "
        j=$((j+1))
    done
    done
    # gnuplot -e "${gnuplot_command}; pause(-1)"
    local name_file_out="YCSB-${_workload}_recordcount_${COUNTRECORDS}-batchsize_${BATCHSIZE}"
    gnuplot -e "set terminal png size 800,600; set output '${OUT_FIGURES}/${name_file_out}.png'; ${gnuplot_command}"

}

function usage() {
    echo "collect_long_run.sh <result_dir> [[record|-r] n-records [batch|-b] n-batch]"
    exit 1  
}

# ============================================================================ #
#
# ============================================================================ #

# ====================================== #
# include <...>
# [ -e "${__DIR}/../configs/my_setenv.sh" ] && source ${__DIR}/../configs/my_setenv.sh
# ====================================== #

# ===================== #
# parse args

[ -z "${1}" ]   && echo "Give dir where tests are saved" && usage
[ ! -e "${1}" ] && echo "Give a valid directory." && usage
__args=("$@")

BASE_DIRECTORY="${1}"
COUNTRECORDS=5000000
BATCHSIZE=10000

[ -e "${BASE_DIRECTORY}/my_setenv.sh" ] && source ${BASE_DIRECTORY}/my_setenv.sh
source ${__DIR}/../configs/config.sh

for (( index_input_arg=1; index_input_arg<=$#; index_input_arg+=1 )); do
     o=${__args[$index_input_arg]}
     case $o in
         -r|record*)
             _debug "arg[$index_input_arg] = $o"
             index_input_arg=$(($index_input_arg+1))
             _debug "arg[$index_input_arg] = $o"
             COUNTRECORDS=${__args[$index_input_arg]}
             ;;
         -b|batch*)
             _debug "arg[$index_input_arg] = $o"
             index_input_arg=$(($index_input_arg+1))
             _debug "arg[$index_input_arg] = $o"
             BATCHSIZE=${__args[$index_input_arg]}
             ;;
         --force)
             FORCE_DELETE_LAST="true"
             ;;
         *)
             _debug "not expected $o: skip"
             ;;
         
     esac   
done

_debug "BASE_DIRECTORY: ${BASE_DIRECTORY}" "COUNTRECORDS: $COUNTRECORDS" "BATCHSIZE: $BATCHSIZE" 
OUT_BASE_DIR="${BASE_DIRECTORY}"/"data_long_${COUNTRECORDS}_${BATCHSIZE}"
OUT_DIR="${OUT_BASE_DIR}"/data/
OUT_FIGURES="${OUT_BASE_DIR}"/images/


[ -e ${OUT_BASE_DIR} ] && [ "${FORCE_DELETE_LAST}" != "true" ]    && \
   echo "---------------------------------------------------"     && \
   echo "[WARN] Summary exists: use --force to replace it"        && \
   echo "---------------------------------------------------"     && \
   exit 1 

mkdir -p ${OUT_DIR} ${OUT_FIGURES}
rm -f ${OUT_DIR}/* ${OUT_FIGURES}/*
echo "====================================="

collect_number_threads
collect_data_for_workload "workloada" "READ" "UPDATE"
collect_data_for_workload "workloadb" "READ" "UPDATE"
collect_data_for_workload "workloadc" "READ"
collect_data_for_workload "workloadd" "READ" "INSERT"
collect_data_for_workload "workloade" "SCAN" "INSERT"
collect_data_for_workload "workloadf" "READ" "UPDATE" "READ-MODIFY-WRITE"

echo "Data saved in ${OUT_BASE_DIR}"
echo "====================================="

# head ${OUT_DIR}/plot_*
