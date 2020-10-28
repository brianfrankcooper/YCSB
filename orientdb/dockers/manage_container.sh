#!/bin/bash

set -e

__DIR_MANAGE_CONTAINER="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function wait_10_seconds() {
    for i in {1..10}; do
        echo -ne "."
        sleep 1
    done
}

function start_orient_db() {
   docker_id=$(sudo docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=admin orientdb:3.1.1)
   echo "orientdb: $docker_id" >> "${__DIR_MANAGE_CONTAINER}/containers_ids.txt"
   echo "Starting orient db: image number ${docker_id}"
   wait_10_seconds 
   while(true); do
       if [ ! -z "$(curl -s http://localhost:2480 | grep "Studio")" ]; then
           break;     
       fi
       echo -ne "\r                                         \rServer not jet ready: "
       wait_10_seconds 
   done
   echo
   echo "Done"
}

# $1: name container
function stop_continer() {
   local name="$1"
   local id_to_stop="$(grep $name ${__DIR_MANAGE_CONTAINER}/containers_ids.txt | cut -f2 -d':' )"
   echo "Stopping continer $id_to_stop"
   sudo docker stop ${id_to_stop} 
   sudo docker rm ${id_to_stop} 
   sed -i "/$name/d" "${__DIR_MANAGE_CONTAINER}/containers_ids.txt"
}

function execute_command() {
   local name="$1"
   local command_to_exec="$2"
   local id_to_stop="$(grep $name ${__DIR_MANAGE_CONTAINER}/containers_ids.txt | cut -f2 -d':' )"
   echo "executing command '${command_to_exec}' in container $id_to_stop"
   sudo docker exec -t ${id_to_stop} bash -c "${command_to_exec}"

}

# =========================================================================== #
# MAIN 
# =========================================================================== #

# ==================================== #
if [[ "$1" = "start" ]]; then
# ==================================== #
    db_name="$2"
    case "$db_name" in
        orient*)
            start_orient_db
            ;;
    esac  
# ==================================== #
elif [[ "$1" = "stop" ]]; then
# ==================================== #
    db_name="$2"
    case "$db_name" in
        orient*)
            stop_continer "orientdb"
            ;;
    esac  
# ==================================== #
elif [[ "$1" = "ls" ]]; then
# ==================================== #
   echo "Registered containers:"
   cat ${__DIR_MANAGE_CONTAINER}/containers_ids.txt
   echo "Running containers:"
   sudo docker ps
# ==================================== #
elif [[ "$1" = "execute" ]]; then
# ==================================== #
   echo "Registered containers:"
   db_name="$2"
   command_to_exec="${3}"
   execute_command "$db_name" "${command_to_exec}"
else
   echo "$0 [start|stop] [database]"
   echo "possible database:"
   echo "   * orient*"
   echo "-------------------------------"
fi

exit 0

# =========================================================================== #
