#!/bin/bash

# Scalability testing script for different client counts
# This script tests the system with varying numbers of clients per node
# and collects throughput data for analysis

# Output file for results
RESULTS_FILE="scalability_results.txt"
echo "# Client_Count Total_Throughput" > $RESULTS_FILE

# Array of client counts to test (powers of 2 for better log scale visualization)
CLIENT_COUNTS=(1 2 4 8 16 32 64 128 256 512 1024)
# CLIENT_COUNTS=(1 2)

echo "Starting scalability tests..."
echo "Testing with client counts: ${CLIENT_COUNTS[@]}"
echo "======================================"

for clients in "${CLIENT_COUNTS[@]}"; do
    echo ""
    echo "Testing with $clients clients per node..."
    echo "--------------------------------------"
    
    # Run the cluster with specified client count
    output=$(./run-cluster.sh 2 2 "-port 8080" "-clients $clients" 2>&1)
    
    # Extract the total throughput from the output
    throughput=$(echo "$output" | grep "^total" | awk '{print $2}')
    
    if [ -n "$throughput" ]; then
        echo "Clients per node: $clients"
        echo "Total throughput: $throughput op/s"
        
        # Save to results file
        echo "$clients $throughput" >> $RESULTS_FILE
        
        # Also save the full output for detailed analysis
        echo "$output" > "logs/scalability_test_${clients}_clients.log"
    else
        echo "ERROR: Could not extract throughput for $clients clients"
        echo "Output was:"
        echo "$output"
    fi
    
    # Wait a bit between tests to ensure clean state
    sleep 5
done

echo ""
echo "======================================"
echo "Scalability tests completed!"
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "Results summary:"
cat $RESULTS_FILE