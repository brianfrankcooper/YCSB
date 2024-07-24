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
