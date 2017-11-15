#!/bin/bash

HOSTDIR=$(pwd)
INIT_MARKER=$HOSTDIR/hdfs-initialized-indicator

# Remove initialization marker
rm -f $INIT_MARKER

# Start the hdfs service
CONTAINER_ID=$(docker run -d -p 8020:8020 -p 50070:50070 -v $(pwd):/hdfs3 daskdev/hdfs3dev)
export CONTAINER_ID

# Error immediately if this fails
if [ $? -ne 0 ]; then
    echo "Failed starting HDFS container"
    exit 1
fi

# Wait for initialization
CHECK_RUNNING="docker top $CONTAINER_ID"
while [[ $($CHECK_RUNNING) ]] && [[ ! -f $INIT_MARKER ]]
do
    sleep 1
done

# Error out if the container failed starting
if [[ ! $($CHECK_RUNNING) ]]; then
    echo "HDFS startup failed! Logs follow"
    echo "-------------------------------------------------"
    docker logs $CONTAINER_ID
    echo "-------------------------------------------------"
    exit 1
fi

echo "Started HDFS container: $CONTAINER_ID"
