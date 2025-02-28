#!/bin/bash

# constant variables
dir_path="$HOME/bee-sensors"
fmt_date=`date +"%Y-%m-%d_%k-%M"`
JSON_FILE="/etc/build_info.json"
PREVIOUS_VERSION="5.0.19"

# Prompt the user for name and S/N
echo "Enter your name:"
read name

echo "Enter S/N of Bee device:"
read sn

# First copy over the JSON file because it's not LOCAL!
# Need this to figure out what to copy over
scp -r -o StrictHostKeyChecking=no root@192.168.0.10:/etc/build_info.json /usr/share/datalogs/current_cam_ver.json
JSON_FILE="/usr/share/datalogs/current_cam_ver.json"
VERSION=$(grep '"odc-version"' "$JSON_FILE" | sed -E 's/.*"odc-version"[[:space:]]*:[[:space:]]*"([0-9.]+)".*/\1/')


ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "mkdir -p /data/qa_gnss"
scp -r -o StrictHostKeyChecking=no $dir_path/qa_gnss/*.py root@192.168.0.10:/data/qa_gnss
scp -r -o StrictHostKeyChecking=no $dir_path/qa_gnss/*.sh root@192.168.0.10:/data/qa_gnss

# Upload datalogger if running on older version
if [ "$VERSION" == "$PREVIOUS_VERSION" ]; then
  scp -o StrictHostKeyChecking=no $dir_path/qa_gnss/datalogger root@192.168.0.10:/data/qa_gnss
fi

ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "python3 /data/qa_gnss/gnss_auto_qa.py --name \"$name\" --sn \"$sn\""
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "cat /data/qa_gnss_results_\"$name\"_\"$sn\".log"



# Download results based on firmware version
if [ "$VERSION" == "$PREVIOUS_VERSION" ]; then
  echo "Running on previous version"
  scp -r -o StrictHostKeyChecking=no root@192.168.0.10:/data/redis_handler/ /usr/share/datalogs/redis_handler_"$name"_"$sn"_"$fmt_date"
else
  scp -r -o StrictHostKeyChecking=no root@192.168.0.10:/data/recording/redis_handler/ /usr/share/datalogs/redis_handler_"$name"_"$sn"_"$fmt_date"
fi

scp -o StrictHostKeyChecking=no root@192.168.0.10:/data/qa_gnss_results_"$name"_"$sn".log /usr/share/datalogs/redis_handler_"$name"_"$sn"_"$fmt_date"/
ssh -t -o StrictHostKeyChecking=no root@192.168.0.10 "rm -rfv /data/qa_gnss/"

echo "Press any key to exit..."
read -n 1
