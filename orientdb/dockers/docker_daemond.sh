#!/bin/bash

set -e

[ "$1" = "-h" ]     && echo "docker_daemond.sh [-h|start|stop|status|prune]"
[ "$1" = "start" ]  && sudo systemctl start  docker.service
[ "$1" = "stop" ]   && sudo systemctl stop   docker.service
[ "$1" = "status" ] && sudo systemctl status docker.service
[ "$1" = "prune" ]  && sudo docker system prune -a --volumes
