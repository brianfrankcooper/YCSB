#!/bin/bash

# Usage: ./setup_redis_cluster.sh <num_nodes> [base_port]
NUM_NODES=${1:-3}
BASE_PORT=${2:-7000}
BASE_DIR="tmp/redis-cluster"
REDIS_SERVER="redis-server"
REDIS_CLI="redis-cli"
PROPERTIES_FILE="redis.properties"

echo "Setting up $NUM_NODES Redis nodes starting from port $BASE_PORT..."

mkdir -p "$BASE_DIR"
CLUSTER_NODES=""

# Create directories, config files, and launch nodes
for ((i = 0; i < NUM_NODES; i++)); do
  PORT=$((BASE_PORT + i))
  NODE_DIR="$BASE_DIR/$PORT"
  mkdir -p "$NODE_DIR"

  cat > "$NODE_DIR/redis.conf" <<EOF
port $PORT
cluster-enabled yes
cluster-config-file nodes-$PORT.conf
cluster-node-timeout 5000
appendonly yes
dir $NODE_DIR
EOF

  echo "Starting Redis node on port $PORT..."
  $REDIS_SERVER "$NODE_DIR/redis.conf" &
  CLUSTER_NODES+="localhost:$PORT,"
done

# Wait for startup
sleep 3

# Trim trailing comma
CLUSTER_NODES=${CLUSTER_NODES%,}

# Create the cluster
echo "Creating Redis cluster..."
yes yes | $REDIS_CLI --cluster create ${CLUSTER_NODES//,/ } --cluster-replicas 0

# Generate redis.properties
echo "Generating redis.properties file..."
cat > "$PROPERTIES_FILE" <<EOF
redis.cluster=true
redis.cluster.nodes=$CLUSTER_NODES
redis.timeout=2000
EOF

echo "Cluster setup complete. Ports used: $CLUSTER_NODES"
