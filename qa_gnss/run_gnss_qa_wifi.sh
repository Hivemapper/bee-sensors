#!/bin/bash


# constant variables
dir_path="$HOME/bee-sensors"
fmt_date=`date +"%Y-%m-%d_%k-%M"`

# Prompt the user for name and S/N
echo "Enter your name:"
read name

echo "Enter S/N of Bee device:"
read sn

scp -r -o StrictHostKeyChecking=no $dir_path/qa_gnss/ root@192.168.0.10:/data
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "python3 /data/qa_gnss/gnss_auto_qa.py --name \"$name\" --sn \"$sn\""
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "cat /data/qa_gnss_results_\"$name\"_\"$sn\".txt"

#Old locations for saving files
#$HOME/Desktop $HOME/Desktop/redis_handler_"$name"_"$sn"/
scp -o StrictHostKeyChecking=no root@192.168.0.10:/data/qa_gnss_results_"$name"_"$sn"_"$fmt_date".txt /usr/share/datalogs
scp -r -o StrictHostKeyChecking=no root@192.168.0.10:/data/redis_handler/ /usr/share/datalogs/redis_handler_"$name"_"$sn"_"$fmt_date"
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "rm -rfv /data/qa_gnss/"

echo "Press any key to exit..."
read -n 1
