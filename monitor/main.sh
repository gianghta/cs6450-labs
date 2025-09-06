#!/bin/bash

DURATION=${1:-30}

# Check tools
if ! command -v sar &> /dev/null; then
    echo "Error: need install sysstat (sudo apt-get install sysstat)"
    exit 1
fi

echo "Start everything..."

echo "Start monitoring"
bash start_monitoring.sh $DURATION

echo "Analyze daya"
bash analyze_data.sh


echo "Everything finished!"
