#!/bin/bash


# constant variables
dir_path="$HOME/bee-sensors"

# Prompt the user for name and S/N
echo "Enter your name:"
read name

echo "Enter S/N of Bee device:"
read sn

scp -r -o StrictHostKeyChecking=no $dir_path/qa_gnss/ root@192.168.0.10:/data
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "python3 /data/qa_gnss/gnss_auto_qa.py --name \"$name\" --sn \"$sn\""
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "cat /data/qa_gnss_results_\"$name\"_\"$sn\".txt"

scp -o StrictHostKeyChecking=no root@192.168.0.10:/data/qa_gnss_results_"$name"_"$sn".txt $HOME/Desktop

echo "Press any key to exit..."
read -n 1
