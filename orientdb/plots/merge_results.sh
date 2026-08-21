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
# $1 name directory to move
# $2 new name directory
# $3 source directory 
# $4 destination directory

__src_name="${1}"
__dst_name="${2}"
__src_dir="${3}"
__dst_dir="${4}"
__force="${5:-false}"


function usage() {
    echo "merge_results.sh <src-name> <dst-name> <src-dir> <dst-dir> [force]"
    echo "try to merge the results from some new test to the historical one:"
    echo "   src-name: name of the original driver"
    echo "   dst-name: name of the destination driver"
    echo "   src-dir:  base directory to look for new tests"
    echo "   dst-dir:  base directory of tests into which merge the new one."
    echo "   force:    overwrite driver already present"
}

[ -z ${__src_name} ] && usage && exit 1
[ -z ${__src_dir}  ]  && usage && exit 1
[ -z ${__dst_name} ] && usage && exit 1
[ -z ${__dst_dir}  ]  && usage && exit 1

# for f in $(find ${__src_dir} -type d -name ${__src_name}); do
# ls ${__src_dir}/test_* -d
cd ${__src_dir}
for f in $(ls test_* -d | grep "test_"); do
   match_dir=$(echo "$f" | sed 's#test_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_[0-9][0-9]-[0-9][0-9]-[0-9][0-9]_\(.*\)_#\1#') 
   echo "* $f/outputs/${__src_name} => ${match_dir}" 
   for ff in $(ls -d ${__dst_dir}/* | grep "${match_dir}"); do
       [ -e "$ff/outputs/${__dst_name}"  ] && [ -z "${__force}" ]         && echo "[INFO] skipping $f/outputs/${__src_name}" && continue
       [ -e "$ff/outputs/${__dst_name}"  ] && [ "${__force}" = "false"  ] && echo "[INFO] skipping $f/outputs/${__src_name}" && continue
       [ -e "$ff/outputs/${__dst_name}"  ] && [ "${__force}" = "force"  ] && rm -r "$ff/outputs/${__dst_name}"
       echo "-----------------------------------------------"
       echo "cp -r '$f/outputs/${__src_name}' \\" 
       echo "  '$ff/outputs/${__dst_name}'"
       echo "+++++++++++++++++++++++++++++++++++++++++++++++"
       cp -r "$f/outputs/${__src_name}" "$ff/outputs/${__dst_name}"
   done
done

