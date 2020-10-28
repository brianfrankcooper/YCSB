#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
_REPO="${DIR}/../"
_PLOTS_DIR="${DIR}/plots"
__load_batch=1000
# MONGODB_DOCKER_NAME=silly_tesla

# OrientDB class name
DATABASE_NAME="usertable"

function wait_10_seconds() {
    for i in {1..10}; do
        echo -ne "."
        sleep 1
    done
}

# =========================================================================== #
# CLEANUP DATABASES
# =========================================================================== #

function clean_orientdb_collection() {
   # echo "FIXME: This cannot be correct: OOM and extremly long!"
   [ ! -z "${ORIENT_DB_LOCAL_INSTANCE}" ] && ${ORIENT_DB_LOCAL_INSTANCE}/bin/console.sh 'DROP DATABASE remote:localhost/tmp/databases/ycsb root admin' && return
   # FIXME maybe root admin ...
   # sudo docker exec -t $(cat test/dockers/containers_ids.txt | grep orient | cut -d':' -f2) /bin/bash -c "/orientdb/bin/console.sh 'DROP DATABASE plocal:./target/databases/ycsb admin admin'"
   if [[ "${NEW_RUN}" = "True" ]]; then
       echo "Stopping and starting the container ..."
       orientdb_manage "stop"
       orientdb_manage "start"
   fi
}

# =========================================================================== #
# START STOP DATABASES
# =========================================================================== #

# $1 start|stop
function orientdb_manage() {
    if [ "${1}" = "start" ]; then
        if [ ! -z "${ORIENT_DB_LOCAL_INSTANCE}" ] ; then
            ${ORIENT_DB_LOCAL_INSTANCE}/bin/server.sh > /tmp/orient.log 2>&1 &
            wait_10_seconds
            while(true); do
            if [ ! -z "$(curl -s http://localhost:2480 | grep "Studio")" ]; then
               break;
            fi
            echo -ne "\r                                         \rServer not jet ready: "
            wait_10_seconds
   done
            return
        fi
    else
        [ ! -z "${ORIENT_DB_LOCAL_INSTANCE}" ] && \
               ${ORIENT_DB_LOCAL_INSTANCE}/bin/shutdown.sh && \
               return
    fi
    [ ! -z "${ORIENT_DB_LOCAL_INSTANCE}" ] && echo "Nothing to do" && return
    # ================================================ #
    ${DIR}/dockers/manage_container.sh "${1}" orient
    # ================================================ #
}

# $1 workload type
function orientdb() {
    echo "======================================================================================="
    echo "|  ORIENTDB                                                                            |"
    echo "======================================================================================="
    echo "Running $1"
    local threads=" -threads ${THREADS_YCSB}"
    local workload_p=${1}
    local out_load=outputLoad_${1}.txt
    local out_run=outputRun_${1}.txt
    mkdir -p ${DIR}/outputs/orientdb
    ./bin/ycsb load orientdb ${threads} -s -P workloads/$workload_p -s ${file_params} > ${DIR}/outputs/orientdb/${out_load}
    ./bin/ycsb run orientdb ${threads} -s -P workloads/$workload_p -s ${file_params}  > ${DIR}/outputs/orientdb/${out_run}
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
}

# $1: database to use:
function run_workloads() {
    echo "======================================================================================="
    echo "|  $1                                                                                 |"
    echo "======================================================================================="
    # ============================================== #
    local out_load=outputLoad_workloada.txt
    mkdir -p "${DIR}/outputs/$1"
    # ============================================== #
    local extra_par=""
    local threads=" -threads ${THREADS_YCSB}"
    [ "$1" = "docstore" ]     && export DO_NOT_DELETE_THE_TABLE="True"

    echo "* Clean Database ..."
    # ============================================== #
    clean_${1}_collection || true
    # ============================================== #
    echo "* Load A"
    ./bin/ycsb load $1 -threads 8 -p batchsize=${__load_batch} -s -P workloads/workloada ${file_params} ${extra_par} > ${DIR}/outputs/$1/${out_load}
    # ============================================== #
    # RUN WORKLOADS
    for w in "workloada" "workloadb" "workloadc" "workloadf" "workloadd"; do
        echo "* Run $w"
        local out_run=outputRun_${w}.txt
        ./bin/ycsb run "$1" ${threads} -s -P workloads/$w  ${file_params} ${extra_par} > ${DIR}/outputs/$1/${out_run}
    done
    # ============================================== #
    echo "* Clean Database ..."
    # ============================================== #
    clean_${1}_collection || true
    local out_load=outputLoad_workloade.txt
    local out_run=outputRun_workloade.txt
    # ============================================== #
    echo "* Load E"
    ./bin/ycsb load "$1" -threads 8 -p batchsize=${__load_batch} -s -P workloads/workloade ${file_params} ${extra_par} > ${DIR}/outputs/$1/${out_load}
    # ============================================== #
    # RUN WORKLOADS
    echo "* Run E"
    ./bin/ycsb run "$1" ${threads} -s -P workloads/workloade ${file_params} ${extra_par} > ${DIR}/outputs/$1/${out_run}
}

# =============================================== #
#                     MAIN                        #
# =============================================== #

function main() {
   # =============================================================== #
   # include <...>

   for var in "$@"; do

      # At the moment, after disabling, no automatic enabling happens ...
      #  to enable it, see at the end of the loop ...
      [[ "$var" =~ ^run.* ]] && set_system_config_for_benchmark
      # =============================================================== #
      if [ "$var" = "run" ]; then
      # =============================================================== #
         cd ${_REPO}
         rm -fr ${DIR}/outputs/*
         for t_name in ${drivers_test[@]}; do
            "${t_name}_manage" "start" || true
            for w in ${workloads[@]}; do
               $t_name "$w"
            done
            "${t_name}_manage" "stop" || true
         done
      # =============================================================== #
      elif [ "$var" = "run-ts" ]; then
      # =============================================================== #
         cd ${_REPO}
         rm -fr ${DIR}/outputs/*
         for t_name in ${drivers_test[@]}; do
            "${t_name}_manage" "start" || true
            for w in ${workloads_ts[@]}; do
               $t_name "$w"
            done
            "${t_name}_manage" "stop" || true
         done
      # =============================================================== #
      elif [ "$var" = "run-new" ]; then
      # =============================================================== #
         export NEW_RUN="True"
         cd ${_REPO}
         rm -fr ${DIR}/outputs/*
         for t_name in ${drivers_test[@]}; do
            # TODO: hard cleanup for the moment:
            yes | ${DIR}/dockers/docker_daemond.sh prune || true
            "${t_name}_manage" "start" || true
            run_workloads "$t_name"
            "${t_name}_manage" "stop" || true
         done
      # =============================================================== #
      elif [ "$var" = "plot" ]; then
      # =============================================================== #
         cd ${_PLOTS_DIR}
         ${DIR}/plots/tool.sh "plot" "save"
      elif [ "$var" = "multi-plot" ]; then
      # =============================================================== #
         cd ${_PLOTS_DIR}
         ${DIR}/plots/tool.sh "plot" "multi"
      # =============================================================== #
      elif [ "$var" = "jenkins" ]; then
      # =============================================================== #
          echo "Run jenkins build pipeline"
          cd ${_REPO}
          mvn clean package
      # =============================================================== #
      elif [ "$var" = "test" ]; then
      # =============================================================== #
         cd ${_REPO}
         # mvn clean package   # FIXME: maybe just ...
         orientdb_manage start;
         mvn -pl site.ycsb:orientdb-binding -am clean package -Denforcer.skip=true;
         orientdb_manage "stop"

         # TODO: hard cleanup for the moment:
         yes | ${DIR}/dockers/docker_daemond.sh prune || true

      # =============================================================== #
      elif [ "$var" = "archive" ]; then
      # =============================================================== #
         name_2=""
         [ ! -z "${filename_params}" ]  && file_param_plot=${DIR}/configs/${filename_params} && \
                                           name_2=$(grep -v '#' ${file_param_plot} | tr '\n=' '__')
         test_result="${DIR}/results/test_$(date +"%Y-%m-%d_%H-%M-%S")${name_2}"
         [[ ! -e "${DIR}/outputs" ]] && \
               echo "Nothing to do: no result presetnt" && \
               exit 1
         mkdir -p ${test_result}
         rm -rf ${DIR}/plots/data/* ${DIR}/plots/images/*  || true
         # === save ===
         ${DIR}/plots/tool.sh "plot" "save"
         # === archive ===
         mv ${DIR}/outputs      \
            ${DIR}/plots/data   \
            ${DIR}/plots/images \
            "${test_result}"
         cp -f ${DIR}/configs/config.sh             \
               ${DIR}/configs/my_setenv.sh          \
              "${test_result}"
         [ ! -z "${filename_params}" ]         && cp ${DIR}/configs/${filename_params} "${test_result}"
         [ -e "$${DIR}/configs/my_setenv.sh" ] && cp ${DIR}/configs/my_setenv.sh       "${test_result}"
         # === archive ===
         echo "Data saved in ${test_result}"
         echo "update last run"
         rm -rf "${DIR}/results/last"
         mkdir -p "${DIR}/results/last/"
         cp -r "${test_result}"/* "${DIR}/results/last/"
      # =============================================================== #
      elif [ "$var" = "save" ]; then
      # =============================================================== #
         name_2=""
         [ ! -z "${filename_params}" ]  && file_param_plot=${DIR}/configs/${filename_params} && \
                                           name_2=$(grep -v '#' ${file_param_plot} | tr '\n=' '__')
         test_result="${DIR}/results/test_$(date +"%Y-%m-%d_%H-%M-%S")${name_2}"
         [[ ! -e "${DIR}/outputs" ]] && \
               echo "Nothing to do: no result presetnt" && \
               exit 1
         mkdir -p ${test_result}
         rm -rf ${DIR}/plots/data/* ${DIR}/plots/images/*  || true
         # === save ===
         ${DIR}/plots/tool.sh "plot" "save"
         # === archive ===
         cp -r ${DIR}/outputs      \
               ${DIR}/plots/data   \
               ${DIR}/plots/images \
              "${test_result}"
         cp -f ${DIR}/configs/config.sh             \
               ${DIR}/configs/my_setenv.sh          \
              "${test_result}"
         [ ! -z "${filename_params}" ]         && cp ${DIR}/configs/${filename_params} "${test_result}"
         [ -e "$${DIR}/configs/my_setenv.sh" ] && cp ${DIR}/configs/my_setenv.sh       "${test_result}"
      # =============================================================== #
      else
      # =============================================================== #
         echo "usage: $(basename $0) [test|run|run-ts|plot|multi-plot|archive]"
         exit 1
      fi

   done

   # =============================================== #

}

# =============================================== #
# include #
# =============================================== #

[ -e "${DIR}/configs/my_setenv.sh" ] && source ${DIR}/configs/my_setenv.sh
source ${DIR}/configs/config.sh
# file_params

[ "${__HANA_ADM}" = "updateme" ]  && echo "You need to set the value for the variable __HANA_ADM!" && exit 1
[ -z "${__HANA_ADM}" ]            && echo "You need to set the value for the variable __HANA_ADM!" && exit 1


[ ! -z "$filename_params" ]                                   && \
      export file_params="-P ${DIR}/configs/$filename_params" && \
      THREADS_YCSB="$(grep -v '#' ${DIR}/configs/$filename_params |grep "threads" | cut -f2 -d'=')"
[ -z "${THREADS_YCSB}" ] && THREADS_YCSB=1

# =============================================================== #