#!/bin/bash

# continue-monitoring.sh - Continue monitoring from commit 093781a onwards

set -euo pipefail

# Add Go to PATH for older commits
export PATH="/usr/local/go/bin:$PATH"

# Configuration
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${ROOT}/commit-monitoring-results"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
RESULTS_FILE="${RESULTS_DIR}/continue-results-${TIMESTAMP}.csv"

mkdir -p "$RESULTS_DIR"

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo "Current branch: $CURRENT_BRANCH"

# Get list of commits from 093781a onwards (commit 8/26)
echo "Getting list of remaining commits..."
COMMITS=($(git log --oneline --reverse nrkhadem | awk '{print $1}' | tail -n +9))
TOTAL_COMMITS=${#COMMITS[@]}

echo "Found $TOTAL_COMMITS remaining commits to monitor"
echo "Results will be saved to: $RESULTS_FILE"

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

# Function to cleanup on exit
cleanup() {
    echo "Returning to original branch: $CURRENT_BRANCH"
    git checkout "$CURRENT_BRANCH" 2>/dev/null || true
    echo "Cleanup complete."
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Monitor each commit
for i in "${!COMMITS[@]}"; do
    COMMIT_HASH="${COMMITS[$i]}"
    COMMIT_MSG=$(git log --oneline -1 "$COMMIT_HASH" | cut -d' ' -f2-)
    
    echo ""
    echo "=========================================="
    echo "Processing commit $((i+9))/$((TOTAL_COMMITS+8)): $COMMIT_HASH"
    echo "Message: $COMMIT_MSG"
    echo "=========================================="
    
    # Clean up any build artifacts before checkout
    rm -rf bin/ 2>/dev/null || true
    
    # Checkout the commit
    echo "Checking out commit $COMMIT_HASH..."
    if ! git checkout "$COMMIT_HASH" 2>/dev/null; then
        echo "ERROR: Failed to checkout commit $COMMIT_HASH"
        echo "$COMMIT_HASH,\"$COMMIT_MSG\",ERROR,ERROR,ERROR,ERROR,ERROR,$(date)" >> "$RESULTS_FILE"
        continue
    fi
    
    # Check if run-cluster.sh exists in this commit
    if [ ! -f "./run-cluster.sh" ]; then
        echo "WARNING: run-cluster.sh not found in commit $COMMIT_HASH, skipping..."
        echo "$COMMIT_HASH,\"$COMMIT_MSG\",SKIPPED,SKIPPED,SKIPPED,SKIPPED,SKIPPED,$(date)" >> "$RESULTS_FILE"
        continue
    fi
    
    # Make sure the script is executable
    chmod +x ./run-cluster.sh
    
    # Run the cluster test and capture output
    echo "Running cluster test..."
    CLUSTER_OUTPUT=$(timeout 300 ./run-cluster.sh 2>&1)
    CLUSTER_EXIT_CODE=$?
    
    # Check if the output contains performance data (success indicator)
    if echo "$CLUSTER_OUTPUT" | grep -q "total.*op/s" && [ $CLUSTER_EXIT_CODE -eq 0 ]; then
        # Extract performance data
        THROUGHPUT_DATA=$(extract_throughput "$CLUSTER_OUTPUT")
        echo "Performance data: $THROUGHPUT_DATA"
        
        # Save to CSV
        echo "$COMMIT_HASH,\"$COMMIT_MSG\",$THROUGHPUT_DATA,$(date)" >> "$RESULTS_FILE"
        
        # Save detailed output for this commit
        echo "$CLUSTER_OUTPUT" > "${RESULTS_DIR}/commit-${COMMIT_HASH}-output.log"
        
    else
        echo "ERROR: Cluster test failed for commit $COMMIT_HASH (exit code: $CLUSTER_EXIT_CODE)"
        echo "$COMMIT_HASH,\"$COMMIT_MSG\",FAILED,FAILED,FAILED,FAILED,FAILED,$(date)" >> "$RESULTS_FILE"
        # Save failed output for debugging
        echo "$CLUSTER_OUTPUT" > "${RESULTS_DIR}/commit-${COMMIT_HASH}-failed.log"
    fi
    
    echo "Completed commit $COMMIT_HASH"
    
    # Clean up build artifacts after test
    rm -rf bin/ 2>/dev/null || true
done

echo ""
echo "=========================================="
echo "CONTINUED MONITORING COMPLETE!"
echo "=========================================="
echo "Results saved to: $RESULTS_FILE"
echo "Detailed logs saved in: $RESULTS_DIR"
echo ""
echo "Summary of results:"
echo "==================="
cat "$RESULTS_FILE"

echo ""
echo "Returning to original branch: $CURRENT_BRANCH"
git checkout "$CURRENT_BRANCH"
