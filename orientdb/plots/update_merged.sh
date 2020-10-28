#!/bin/bash
set -e

__UPDATE_MERGED_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

[ -z "${1}" ] && echo "provide the directory to be updated" && exit
[ ! -e "${1}" ] && echo "provide a valid directory to be updated" && exit

__dir_to_update="${1}"

mkdir -p "${__dir_to_update}/all_workloads_hists"

for f in $(ls -d ${__dir_to_update}/*); do
    [ ! -z "$(basename $f | grep -E "^all_workloads_hists")"  ] && continue
    [ ! -z "$(basename $f | grep -E "^data_long_")"  ] && continue
    [ ! -z "$(basename $f | grep -E "^my_setenv.sh")"  ] && continue
    echo "============================================================="
    echo "$f"
    ${__UPDATE_MERGED_DIR}/tool.sh plot save -d "$f"
    echo "============================================================="
done 

for f in $(ls -d ${__dir_to_update}/*); do
    [ ! -z "$(basename $f | grep -E "^all_workloads_hists")"  ] && continue
    [ ! -z "$(basename $f | grep -E "^data_long_")"  ] && continue
    [ ! -z "$(basename $f | grep -E "^my_setenv.sh")"  ] && continue
    file_name=$(basename $f)
    new_file_name=$(echo "${file_name}" | sed 's#test_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9]-[0-9][0-9]-[0-9][0-9]_##') 
    echo "copy ${new_file_name}.png"
    cp $f/images/workload_all.png ${__dir_to_update}/all_workloads_hists/${new_file_name}.png
done 


# TODO: Add
# collect_data_long_run.sh
for f in $(ls -d ${__dir_to_update}/data_long_*); do 
   name=$(basename $f)
   name=$(echo $name | sed 's/data_long//')
   records=$(echo $name | sed 's/_\([0-9].*\)_.*/\1/')
   batch=$(echo $name | sed 's/.*_\([0-9].*\)/\1/') 
   echo "records: $records" 
   echo "batch: $batch" 
   ${__UPDATE_MERGED_DIR}/collect_data_long_run.sh ${__dir_to_update} -b $batch -r $records --force
done

# if [ ! -z "${2}" ] ; then
   # __dir_dst_plots="${2}"
   # echo "Saving plots to ${__dir_dst_plots}"
   # [ -e "${2}" ] && echo "Directory ${__dir_dst_plots}  exists. Exiting" && exit 1
   # mkdir -p "${__dir_dst_plots}"
# fi
