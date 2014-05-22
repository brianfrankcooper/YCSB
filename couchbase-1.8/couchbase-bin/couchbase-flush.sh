#!/bin/sh
BASE_DIR=`dirname $0`
. $BASE_DIR/couchbase-env.sh

HOSTS=$(echo $HOSTS | tr "," "\n")
HOSTS=(${HOSTS// / })

for (( i = o ; i < ${#HOSTS[@]} ; i++ )) do
  echo "Flushing cache at host: ${HOSTS[i]}"
  /opt/couchbase/bin/couchbase-cli bucket-flush --cluster=${HOSTS[i]}:${DEFAULT_PORT} \
    --user=${USER_NAME} --password=${USER_PASSWORD} \
    --bucket=${BUCKET_NAME}
done
