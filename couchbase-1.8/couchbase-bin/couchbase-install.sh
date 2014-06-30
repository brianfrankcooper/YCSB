#!/bin/sh
BASE_DIR=`dirname $0`
. $BASE_DIR/couchbase-env.sh

HOSTS=$(echo $HOSTS | tr "," "\n")
HOSTS=(${HOSTS// / })

if [ -z "$PRIMARY_HOST" ]
then
    PRIMARY_HOST=${HOSTS[0]}
fi

echo "Creating cluster with controller ${PRIMARY_HOST}"
/opt/couchbase/bin/couchbase-cli cluster-init --cluster=${PRIMARY_HOST}:${DEFAULT_PORT} \
  --cluster-init-ramsize=${CLUSTER_RAMSIZE} \
  --cluster-init-username=${USER_NAME} \
  --cluster-init-password=${USER_PASSWORD}
/opt/couchbase/bin/couchbase-cli node-init --cluster=${PRIMARY_HOST}:${DEFAULT_PORT} \
  --user=${USER_NAME} --password=${USER_PASSWORD} \
  --node-init-data-path=/var/lib/couchbase

for (( i = 1 ; i < ${#HOSTS[@]} ; i++ )) do
  if [ $i -gt 0 ]
  then
    echo "Adding secondary host: ${HOSTS[i]}"
    /opt/couchbase/bin/couchbase-cli server-add --cluster=${PRIMARY_HOST}:${DEFAULT_PORT} \
      --user=${USER_NAME} --password=${USER_PASSWORD} \
      --server-add=${HOSTS[i]}:${DEFAULT_PORT}
  fi
done

echo "Creating bucket ${BUCKET_NAME} of type ${BUCKET_TYPE}"
/opt/couchbase/bin/couchbase-cli bucket-create --cluster=${PRIMARY_HOST}:${DEFAULT_PORT} \
  --user=${USER_NAME} --password=${USER_PASSWORD} \
  --bucket=${BUCKET_NAME} \
  --bucket-type=${BUCKET_TYPE} \
  --bucket-ramsize=${BUCKET_RAMSIZE} \
  --bucket-replica=${BUCKET_REPLICAS}

echo "Rebalancing cluster"
/opt/couchbase/bin/couchbase-cli rebalance --cluster=${PRIMARY_HOST}:${DEFAULT_PORT} \
  --user=${USER_NAME} --password=${USER_PASSWORD}
