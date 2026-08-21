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
__DIR_TOOL="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export DO_SET_DATA_DIR=""

run_command="${1}"
type_plot="${2}"

function usage() {
   echo "$0 [collect|plot] [-d 'directory-with-data']"
   exit 1
}

if [ "${run_command}" == "collect" ]; then
   if [ "${2}" = "-d" ]; then 
      [ -z "${3}" ] && usage && exit 1
      export DO_SET_DATA_DIR="${3}"
      echo "Setting data directory to ${DO_SET_DATA_DIR}"
   fi
   ${__DIR_TOOL}/collect_data.sh
   exit 0
fi

if [ "${run_command}" == "plot" ]; then
   type_plot="${2}"
   if [ "${3}" = "-d" ]; then 
      [ -z "${4}" ] && usage && exit 1
      export DO_SET_DATA_DIR=${4}
      echo "Setting data directory to ${DO_SET_DATA_DIR}"
   fi
   ${__DIR_TOOL}/collect_data.sh
   ${__DIR_TOOL}/plot.sh ${type_plot} 
   exit 0
fi

exit 1

