#!/bin/bash

echo "Forcefully killing all Redis server processes..."
pkill -9 redis-server

echo "Waiting briefly to ensure processes terminate..."
sleep 1

echo "Cleaning up Redis cluster data directories..."
rm -rf tmp/redis-cluster/*

echo "Teardown complete. Redis processes killed and data wiped."
