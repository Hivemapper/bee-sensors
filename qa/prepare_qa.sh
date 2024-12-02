#!/bin/bash

set -x
GOOS=linux GOARCH=arm64 go build ./cmd/datalogger && \
ssh -t bee 'systemctl stop odc-api' && \
ssh -t bee 'systemctl stop redis' && \
ssh -t bee 'systemctl stop redis-handler' && \
ssh -t bee 'systemctl stop hivemapper-data-logger' && \
ssh -t bee 'rm -rfv /data/redis_handler/*.db*' && \
ssh -t bee 'rm -fv /data/hivemapper-data-logger.log' && \
ssh -t bee 'mount -o remount,rw /' &&
scp datalogger bee:/opt/dashcam/bin && \
sleep 2 && \
ssh -t bee 'systemctl start hivemapper-data-logger'
ssh -t bee 'systemctl start redis'
ssh -t bee 'systemctl start redis-handler'
ssh -t bee 'tail -f /data/hivemapper-data-logger.log'