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

if [ -z "${drivers_test}" ]; then

   declare -a drivers_test=(
      "orientdb"
   )
fi

if [[ -z ${THREADS_YCSB} ]]; then
    export THREADS_YCSB=1
fi

# ==================================================== #
