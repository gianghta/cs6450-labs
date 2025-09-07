#!/bin/bash

# test-monitoring.sh - Simple test to verify monitoring script works correctly

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${ROOT}/commit-monitoring-results"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
RESULTS_FILE="${RESULTS_DIR}/test-results-${TIMESTAMP}.csv"

mkdir -p "$RESULTS_DIR"

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo "Current branch: $CURRENT_BRANCH"

# Create CSV header
echo "commit_hash,commit_message,throughput_ops_per_sec,node0_ops,node1_ops,node2_ops,node3_ops,timestamp" > "$RESULTS_FILE"

# Function to extract throughput from run-cluster.sh output
extract_throughput() {
    local output="$1"
    local total_throughput=$(echo "$output" | grep "total" | grep "op/s" | awk '{print $2}' | tr -d 'op/s' | tr -d ',')
    local node0_ops=$(echo "$output" | grep "node0 median" | awk '{print $3}' | tr -d 'op/s' | tr -d ',')
    local node1_ops=$(echo "$output" | grep "node1 median" | awk '{print $3}' | tr -d 'op/s' | tr -d ',')
    local node2_ops=$(echo "$output" | grep "node2 median" | awk '{print $3}' | tr -d 'op/s' | tr -d ',')
    local node3_ops=$(echo "$output" | grep "node3 median" | awk '{print $3}' | tr -d 'op/s' | tr -d ',')
    
    echo "$total_throughput,$node0_ops,$node1_ops,$node2_ops,$node3_ops"
}

# Test current commit
COMMIT_HASH=$(git rev-parse HEAD)
COMMIT_MSG=$(git log --oneline -1 | cut -d' ' -f2-)

echo "Testing current commit: $COMMIT_HASH"
echo "Message: $COMMIT_MSG"

# Run the cluster test and capture output
echo "Running cluster test..."
CLUSTER_OUTPUT=$(timeout 300 ./run-cluster.sh 2>&1)
CLUSTER_EXIT_CODE=$?

# Check if the output contains performance data (success indicator)
if echo "$CLUSTER_OUTPUT" | grep -q "total.*op/s" && [ $CLUSTER_EXIT_CODE -eq 0 ]; then
    # Extract performance data
    THROUGHPUT_DATA=$(extract_throughput "$CLUSTER_OUTPUT")
    echo "✅ SUCCESS: Performance data: $THROUGHPUT_DATA"
    
    # Save to CSV
    echo "$COMMIT_HASH,\"$COMMIT_MSG\",$THROUGHPUT_DATA,$(date)" >> "$RESULTS_FILE"
    
    # Save detailed output for this commit
    echo "$CLUSTER_OUTPUT" > "${RESULTS_DIR}/test-${COMMIT_HASH}-output.log"
    
else
    echo "❌ FAILED: Cluster test failed (exit code: $CLUSTER_EXIT_CODE)"
    echo "$COMMIT_HASH,\"$COMMIT_MSG\",FAILED,FAILED,FAILED,FAILED,FAILED,$(date)" >> "$RESULTS_FILE"
    # Save failed output for debugging
    echo "$CLUSTER_OUTPUT" > "${RESULTS_DIR}/test-${COMMIT_HASH}-failed.log"
fi

echo ""
echo "Results saved to: $RESULTS_FILE"
echo "Summary:"
cat "$RESULTS_FILE"
