#!/bin/bash

systemctl stop map-ai
echo "Stopped map-ai"
systemctl stop odc-api
echo "Stopped odc-api"
systemctl stop hivemapper-data-logger
echo "Stopped hivemapper-data-logger"
systemctl stop redis
echo "Stopped redis"
systemctl stop redis-handler
echo "Stopped redis-handler"

rm -rv /data/cache/*
echo "Cleared cache"    
rm /data/landmarks/*
echo "Cleared landmarks"
rm -rv /data/optical_flow/*
echo "Cleared optical flow assets"

# remove old envirnmental variables
rm -rv /etc/systemd/system/redis-handler.service.d
rm -rv /etc/systemd/system/map-ai.service.d
rm -v /etc/profile.d/set_dbug_mode.sh
rm -v /etc/profile.d/set_debug_mode.sh
rm -v /etc/profile.d/set_redis_db_path.sh

sleep 1
mount -o remount,rw /
echo "Remounted rootfs as read-write"
sleep 1

# change to debug mode
sed -i 's/"DEBUG_MODE":0/"DEBUG_MODE":1/' /opt/dashcam/bin/config.json
cat /opt/dashcam/bin/config.json
echo "Changed to debug mode"

rm -v /data/recording/framekm/*
echo "Cleared framekms"
rm -v /data/recording/metadata/*
echo "Cleared metadata"
rm -v /data/recording/csv/*
echo "Cleared csv"
rm -v /data/recording/data-logger*
echo "Cleared data-logger DB"
rm -rv /data/redis_handler/
echo "Cleared redis DB"
rm -v /data/map-ai.log
echo "Cleaned up map-ai log"
rm -v /data/recording/odc-api.log
echo "Cleaned up odc-api log"

echo "Ready."