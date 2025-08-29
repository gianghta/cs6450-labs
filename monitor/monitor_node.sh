#!/bin/bash

# Configurations
NODE_NAME=${1:-"node_$(hostname)"}
DURATION=${2:-120}  # Default duration
INTERVAL=1          # Sample interval
OUTPUT_DIR="./monitoring_data"

# Create the output dir
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/${NODE_NAME}"

# Get current timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "Start to monitor node: $NODE_NAME"
echo "Monitoring duration: $DURATION seconds"
echo "Output dir: $OUTPUT_DIR"

# Start all monitoring processes
echo "Start CPU monitoring..."
sar -u $INTERVAL $DURATION > "$OUTPUT_DIR/${NODE_NAME}/${NODE_NAME}_cpu_${TIMESTAMP}.log" &
CPU_PID=$!

echo "Start memory monitoring..."
sar -r $INTERVAL $DURATION > "$OUTPUT_DIR/${NODE_NAME}/${NODE_NAME}_memory_${TIMESTAMP}.log" &
MEM_PID=$!

echo "Start network monitoring..."
sar -n DEV $INTERVAL $DURATION > "$OUTPUT_DIR/${NODE_NAME}/${NODE_NAME}_network_${TIMESTAMP}.log" &
NET_PID=$!

echo "Start disk IO monitoring..."
sar -d $INTERVAL $DURATION > "$OUTPUT_DIR/${NODE_NAME}/${NODE_NAME}_disk_${TIMESTAMP}.log" &
DISK_PID=$!

# Wait till everyone finished
wait $CPU_PID
wait $MEM_PID
wait $NET_PID
wait $DISK_PID

echo "None $NODE_NAME monitoring finished!"
echo "Data stored at: $OUTPUT_DIR"