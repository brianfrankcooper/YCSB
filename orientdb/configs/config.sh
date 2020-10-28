if [ -z "$workloads" ]; then
   declare -a workloads=(
      "workloada"
      "workloadb"
      "workloadc"
      "workloadd"
      "workloade"
      "workloadf"
   )
fi

if [ -z "${workloads_ts}" ]; then
   declare -a workloads_ts=(
      "tsworkloada"
   )
fi

if [ -z "${__HANA_ADM}" ]; then
    __HANA_ADM="updateme"
fi

if [ -z "${drivers_test}" ]; then
      # "cassandra"
   declare -a drivers_test=(
      "mongodb" 
      "docstore"
      "orientdb"
      "postgrenosql"
      "redis"
   )
     # "cassandra"
fi

# ==================================================== #
# might be better to define them in your my_setenv.sh  #
# ==================================================== #
# to change the connection to hana
# if [[ -z "${HANA_HOST}" ]]; then
    # not localhost!!!
    # export HANA_HOST=""
# fi
# if [[ -z "${HANA_INSTANCE}" ]]; then
#    export HANA_INSTANCE=""
# fi

# if [[ -z "${filename_params}" ]]; then
#    export filename_params="1Mio.dat"
# fi

# if [[ -z "${SET_PROFILER_BOOSS}" ]]; then
#    export SET_PROFILER_BOOSS="booss"
#    export SET_PROFILER_BOOSS=""
#    export YCSB_DO_HANA_RESTART="true"
# fi

# if [[ -z "${YCSB_DO_HANA_RESTART}" ]]; then
#    export YCSB_DO_HANA_RESTART="true"
# fi

# if [[ -z "${HANA_REPOSITORY}" ]]; then
#    export HANA_REPOSITORY="/path/to/hana/git"
# fi

if [[ -z ${THREADS_YCSB} ]]; then
    export THREADS_YCSB=1
fi


# ==================================================== #
