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
rm -rv /data/landmarks/
echo "Cleared landmarks"
rm -rv /data/optical_flow/
echo "Cleared optical flow assets"

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
rm -v /data/recording/*.log*
echo "Cleaned up old logs"
rm -v /data/recording/lte-persist/*
echo "Cleaned up lte-persist"
rm -v /data/recording/lte-cookie-persist/*
echo "Cleaned up lte-cookie-persist"

echo "Ready."
