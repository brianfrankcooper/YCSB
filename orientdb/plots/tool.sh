#!/bin/bash

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

