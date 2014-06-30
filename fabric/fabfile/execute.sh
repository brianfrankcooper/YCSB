#!/bin/bash

# the lockfile contains PID of the current process
LOCKFILE="./`basename $0`.lock"
if [ -e ${LOCKFILE} ] && kill -0 `cat ${LOCKFILE}`; then
    echo "already running, kill `cat ${LOCKFILE}`"
    kill -9 `cat ${LOCKFILE}`
    # continue
fi

# make sure the lockfile is removed when we exit and then claim it
trap "rm -f ${LOCKFILE}; exit" INT TERM EXIT
echo $$ > ${LOCKFILE}

######################################
# do_stuff

echo "pid $$"
# echo "/opt/ycsb/bin/ycsb $*"
/opt/ycsb/bin/ycsb $*

# ffuts_od
######################################

rm -f ${LOCKFILE}
