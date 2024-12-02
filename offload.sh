#!/bin/bash

if [ -f ~/.ssh/config ]; then
  echo "Bee ssh config exists"
  if grep -q "HostName test" ~/.ssh/config; then 
    echo "Host bee already exists"
    else
      echo "Host bee not found yet"
      echo "" >> ~/.ssh/config
      echo "Host bee" >> ~/.ssh/config
      echo "  HostName 192.168.0.10" >> ~/.ssh/config
      echo "  User root" >> ~/.ssh/config
      echo "  StrictHostKeyChecking no" >> ~/.ssh/config
    fi       
  else
    echo "ssh config file doesn't yet exist"
    echo "Host bee" >> ~/.ssh/config
    echo "  HostName 192.168.0.10" >> ~/.ssh/config
    echo "  User root" >> ~/.ssh/config
    echo "  StrictHostKeyChecking no" >> ~/.ssh/config
    
fi

ssh bee 'systemctl stop map-ai'
ssh bee 'systemctl stop odc-api'
ssh bee 'systemctl stop hivemapper-data-logger'
ssh bee 'systemctl stop redis'
ssh bee 'systemctl stop redis-handler'
mkdir -v './bee_'$1
scp -r bee:/data/landmarks './bee_'$1
scp -r bee:/data/optical_flow './bee_'$1
scp -r bee:/data/cache './bee_'$1
scp bee:/data/redis_handler/* './bee_'$1
scp bee:/data/recording/*.db* './bee_'$1
scp bee:/data/recording/*.log* './bee_'$1
scp bee:/data/*.log* './bee_'$1