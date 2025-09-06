#!/bin/bash

# Node configuration
declare -A NODES
NODES["server1"]="node0"
NODES["server2"]="node1"
NODES["client1"]="node2"
NODES["client2"]="node3"

DURATION=${1:-30}  # Monitor duration
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Start monitoring in every node..."
echo "Monitoring duration: $DURATION seconds"

# Start monitoring in every node
for node_name in "${!NODES[@]}"; do
    node_addr=${NODES[$node_name]}
    echo "Start node monitor: $node_name"

    ssh "$node_addr" "cd $SCRIPT_DIR && nohup ./monitor_node.sh $node_name $DURATION > monitor.log 2>&1 &" &
done

echo "Everyone started"
echo "Wait till finish ($DURATION 秒)..."

# Wait till finish
sleep $(($DURATION + 10))

echo "Finished!"
