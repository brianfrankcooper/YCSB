CQL="DROP KEYSPACE IF EXISTS ycsb;
CREATE KEYSPACE IF NOT EXISTS ycsb WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND DURABLE_WRITES = true;
USE ycsb;
DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (metric text, tags text, valuetime timestamp, value double, PRIMARY KEY (metric, tags, valuetime)) WITH CLUSTERING ORDER BY (tags ASC, valuetime ASC);"

until echo $CQL | cqlsh; do
  echo "cqlsh: Cassandra is unavailable to initialize - will retry later"
  sleep 2
done &

exec /docker-entrypoint.sh "$@"
