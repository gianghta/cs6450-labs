#!/bin/bash

# Script to analyze monitoring data and extract median values
# Creates a markdown table with CPU, Memory, and Network medians

# Function to calculate median
calculate_median() {
    local sorted_values=$(echo "$1" | tr ' ' '\n' | sort -n)
    local count=$(echo "$sorted_values" | wc -l)
    
    if [ $count -eq 0 ]; then
        echo "N/A"
        return
    fi
    
    if [ $((count % 2)) -eq 1 ]; then
        # Odd number of values - return middle value
        local middle=$((count / 2 + 1))
        local value=$(echo "$sorted_values" | sed -n "${middle}p")
        # Format to ensure leading zero for values < 1
        printf "%.2f" "$value"
    else
        # Even number of values - return average of two middle values
        local middle1=$((count / 2))
        local middle2=$((middle1 + 1))
        local val1=$(echo "$sorted_values" | sed -n "${middle1}p")
        local val2=$(echo "$sorted_values" | sed -n "${middle2}p")
        # Use printf to format with leading zero
        printf "%.2f" $(echo "scale=2; ($val1 + $val2) / 2" | bc)
    fi
}

# Function to extract CPU utilization values and calculate median
get_cpu_median() {
    local file=$1
    if [ ! -f "$file" ]; then
        echo "N/A"
        return
    fi
    
    # Extract %idle values (last column), skip headers and average line
    # Then calculate utilization as 100 - idle
    local util_values=$(awk '/^[0-9]+:[0-9]+:[0-9]+.*all/ {print 100 - $NF}' "$file")
    
    if [ -z "$util_values" ]; then
        echo "N/A"
        return
    fi
    
    local median=$(calculate_median "$util_values")
    echo "$median"
}

# Function to extract memory usage values and calculate median
get_memory_median() {
    local file=$1
    if [ ! -f "$file" ]; then
        echo "N/A"
        return
    fi
    
    # Extract %memused values (5th column), skip headers and average line
    local mem_values=$(awk '/^[0-9]+:[0-9]+:[0-9]+/ && !/Average/ {print $6}' "$file")
    
    if [ -z "$mem_values" ]; then
        echo "N/A"
        return
    fi
    
    local median=$(calculate_median "$mem_values")
    echo "$median"
}

# Function to extract network utilization values and calculate median
get_network_median() {
    local file=$1
    if [ ! -f "$file" ]; then
        echo "N/A"
        return
    fi
    
    # Extract %ifutil values for eno1d1 interface (last column)
    # eno1d1 seems to be the main data interface based on the high traffic
    local net_values=$(awk '/eno1d1/ && /^[0-9]+:[0-9]+:[0-9]+/ && !/Average/ {print $NF}' "$file")
    
    if [ -z "$net_values" ]; then
        echo "N/A"
        return
    fi
    
    local median=$(calculate_median "$net_values")
    echo "$median"
}

# Main execution
echo "Analyzing performance monitoring data..."
echo ""

# Define the base directory
BASE_DIR="./monitoring_data"

# Check if directory exists
if [ ! -d "$BASE_DIR" ]; then
    echo "Error: monitoring_data directory not found!"
    exit 1
fi

# Create results file
RESULTS_FILE="performance_summary.md"

# Write header
cat > "$RESULTS_FILE" << EOF
# Performance Monitoring Summary

Generated on: $(date)

## Median Values

| Node | CPU Median (%) | Memory Median (%) | Network Median (%) |
|------|--------------|-----------------|------------------|
EOF

# Process each node
for node_dir in "$BASE_DIR"/*; do
    if [ -d "$node_dir" ]; then
        node_name=$(basename "$node_dir")
        
        # Convert node name to display format
        display_name=""
        case $node_name in
            client1) display_name="Client 1" ;;
            client2) display_name="Client 2" ;;
            server1) display_name="Server 1" ;;
            server2) display_name="Server 2" ;;
            *) display_name="$node_name" ;;
        esac
        
        echo "Processing $display_name..."
        
        # Get file paths
        cpu_file="$node_dir/${node_name}_cpu_"*.log
        mem_file="$node_dir/${node_name}_memory_"*.log
        net_file="$node_dir/${node_name}_network_"*.log
        
        # Calculate medians
        cpu_median=$(get_cpu_median $cpu_file)
        mem_median=$(get_memory_median $mem_file)
        net_median=$(get_network_median $net_file)
        
        # Format output with % symbol
        if [ "$cpu_median" != "N/A" ]; then
            cpu_median="${cpu_median}%"
        fi
        if [ "$mem_median" != "N/A" ]; then
            mem_median="${mem_median}%"
        fi
        if [ "$net_median" != "N/A" ]; then
            net_median="${net_median}%"
        fi
        
        # Write to results file
        echo "| $display_name | $cpu_median | $mem_median | $net_median |" >> "$RESULTS_FILE"
    fi
done

echo "" >> "$RESULTS_FILE"
echo "## Notes" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"
echo "- **CPU Util**: CPU utilization percentage (100 - %idle)" >> "$RESULTS_FILE"
echo "- **Memory Used**: Percentage of total memory in use" >> "$RESULTS_FILE"
echo "- **Network Util**: Network interface utilization percentage (eno1d1 interface)" >> "$RESULTS_FILE"

echo ""
echo "Analysis complete! Results saved to: $RESULTS_FILE"
echo ""
echo "Summary:"
cat "$RESULTS_FILE"