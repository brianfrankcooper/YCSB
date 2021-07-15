#!/usr/bin/env bash

set -o xtrace

unset LANG
unset LANGUAGE
LC_ALL=en_US.utf8
export LC_ALL

# check out corfu branch
# build corfu
# create cluster
# run ycsb
# export results

if [[ ! -v GIT_BRANCH ]]
then
  echo "Not a pull request!"
  exit 0
fi

CORFU_BRANCH=${GIT_BRANCH}
CORFU_REPO_URL=https://github.com/CorfuDB/CorfuDB.git

TOP_DIR=$(cd $(dirname "$0") && pwd)
START_TIME=$(date +%s)

git clone --single-branch --branch $CORFU_BRANCH $CORFU_REPO_URL
(cd $TOP_DIR/CorfuDB; mvn clean install -DskipTests -T8)

$TOP_DIR/CorfuDB/bin/corfu_server -ms 9000 &> server.log &
SERVER_PID=$!
# Run test
time ./bin/ycsb run corfudb -threads 8 -s -P workloads/workloada -p corfu.connection="localhost:9000" -p corfu.singleclient="true" -p exportfile=stats.json
kill SERVER_PID
./process_stats.py stats.json

# commit and date

#rm -rf $TOP_DIR/corfu_workspace
#mkdir $TOP_DIR/corfu_workspace
#cd corfu_workspace
#git clone --single-branch --branch $CORFU_BRANCH $CORFU_REPO_URL
#cd CorfuDB
#mvn clean install -DskipTests -T8
#pkill -f 'infrastructure-0.3.1-SNAPSHOT-shaded.jar'
#mkdir n0
#nohup ./bin/corfu_server -s -l n0 -N 9000 2>&1 &
#cd $TOP_DIR
#time ./bin/ycsb run corfudb -threads 8 -s -P workloads/workloada -p corfu.connection="localhost:9000"
#pkill -f 'infrastructure-0.3.1-SNAPSHOT-shaded.jar'
#mv stats.json stats-$(date "+%Y.%m.%d-%H.%M.%S").json
