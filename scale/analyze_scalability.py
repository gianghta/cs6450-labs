#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
import sys

def analyze_scalability(filename='../scalability_results.txt'):
    """
    Analyze scalability results and create visualization
    """
    # Read the results
    clients = []
    throughputs = []
    
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:  # Skip header
                if line.strip():
                    parts = line.strip().split()
                    if len(parts) == 2:
                        clients.append(int(parts[0]))
                        throughputs.append(int(parts[1]))
    except FileNotFoundError:
        print(f"Error: {filename} not found. Please run the scalability test script first.")
        sys.exit(1)
    
    if not clients:
        print("No data found in results file.")
        sys.exit(1)
    
    # Calculate total clients (clients per node * number of client nodes)
    # From the output, we have 2 client nodes
    total_clients = [c * 2 for c in clients]
    
    # Calculate log2 of client counts for x-axis
    log_clients = np.log2(total_clients)
    
    # Create the plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot 1: Log scale throughput vs clients
    ax1.plot(log_clients, throughputs, 'b-o', linewidth=2, markersize=8)
    ax1.set_xlabel('Log₂(Total Number of Clients)', fontsize=12)
    ax1.set_ylabel('Total Throughput (op/s)', fontsize=12)
    ax1.set_title('System Scalability: Throughput vs Client Count (Log Scale)', fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Add value labels on points
    for i, (x, y) in enumerate(zip(log_clients, throughputs)):
        ax1.annotate(f'{throughputs[i]:,}', 
                    xy=(x, y), 
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=9)
    
    # Set x-axis ticks to show actual client counts
    ax1.set_xticks(log_clients)
    ax1.set_xticklabels([str(tc) for tc in total_clients], rotation=45)
    
    # Plot 2: Efficiency analysis (throughput per client)
    throughput_per_client = [t/tc for t, tc in zip(throughputs, total_clients)]
    ax2.plot(log_clients, throughput_per_client, 'r-s', linewidth=2, markersize=8)
    ax2.set_xlabel('Log₂(Total Number of Clients)', fontsize=12)
    ax2.set_ylabel('Throughput per Client (op/s)', fontsize=12)
    ax2.set_title('Efficiency: Throughput per Client', fontsize=14)
    ax2.grid(True, alpha=0.3)
    
    # Set x-axis ticks
    ax2.set_xticks(log_clients)
    ax2.set_xticklabels([str(tc) for tc in total_clients], rotation=45)
    
    plt.tight_layout()
    
    # Print analysis results
    print("\n" + "="*60)
    print("SCALABILITY ANALYSIS RESULTS")
    print("="*60)
    print(f"\n{'Clients/Node':<15} {'Total Clients':<15} {'Throughput':<15} {'Per-Client':<15}")
    print("-"*60)
    
    for i in range(len(clients)):
        print(f"{clients[i]:<15} {total_clients[i]:<15} {throughputs[i]:<15,} {throughput_per_client[i]:<15,.1f}")
    
    # Calculate scalability metrics
    print("\n" + "="*60)
    print("SCALABILITY METRICS")
    print("="*60)
    
    # Find peak throughput
    max_throughput = max(throughputs)
    max_idx = throughputs.index(max_throughput)
    optimal_clients = total_clients[max_idx]
    
    print(f"\nPeak Throughput: {max_throughput:,} op/s")
    print(f"Optimal Client Count: {optimal_clients} total clients ({clients[max_idx]} per node)")
    
    # Calculate scalability factor (comparing minimum to maximum client count)
    if len(throughputs) > 1:
        scalability_factor = throughputs[-1] / throughputs[0]
        client_increase = total_clients[-1] / total_clients[0]
        efficiency = scalability_factor / client_increase
        
        print(f"\nScalability Factor: {scalability_factor:.2f}x")
        print(f"Client Increase: {client_increase:.0f}x")
        print(f"Scalability Efficiency: {efficiency:.2%}")
    
    # Identify saturation point (where throughput stops increasing significantly)
    threshold = 0.05  # 5% increase threshold
    saturation_point = None
    for i in range(1, len(throughputs)):
        if i > 0:
            increase = (throughputs[i] - throughputs[i-1]) / throughputs[i-1]
            if increase < threshold:
                saturation_point = total_clients[i-1]
                break
    
    if saturation_point:
        print(f"\nSaturation Point: ~{saturation_point} total clients")
        print("(Where throughput increase becomes < 5%)")
    
    # Save the plot
    plt.savefig('scalability_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\nPlot saved as: scalability_analysis.png")
    
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_scalability(sys.argv[1])
    else:
        analyze_scalability()