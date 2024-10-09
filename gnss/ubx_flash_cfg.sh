#!/bin/bash
#
# flashCfg.sh
#
# Script to flash UBlox configuration
#
# Copyright 2024 Hellbender Inc.
# Some rights reserved.
#
# Changelog:
# Author Email, Date,     , Comment
# niessl      , 2024-08-14, Updated File
#

ubxtoolCmd="/home/<your-gpsd-directory>/gpsd-3.25/gpsd-3.25/clients/ubxtool"
address="192.168.0.10"
port="9090"

sigAttn="255"
sigMinCNo="2"

while getopts 'a:m:p:s:u:h' opt; do
  case "$opt" in
    a)
      address="${OPTARG}"
      ;;

    m)
      sigMinCNo="${OPTARG}"
      ;;

    p)
      port="${OPTARG}"
      ;;

    s)
      sigAttn="${OPTARG}"
      ;;

    u)
      ubxtoolCmd="${OPTARG}"
      ;;

    ?|h)
      echo "Usage: $(basename $0) [-a address] [-m minCNo] [-p port] [-s sigattncomp] [-u ubxPath]"
      exit 1
      ;;
  esac
done

echo "Using address ${address}"
echo "Using port ${port}"
echo "Using sigattncomp ${sigAttn}"

echo Pushing cfg to $address:$port

# NEW MESSAGES

#CFG-MSGOUT-UBX_NAV_COV_UART1: set UBX-NAV-COV message rate
$ubxtoolCmd --command 06,8A,00,07,00,00,84,00,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_TIM_TP_UART1: set UBX-TIM-TP message rate
$ubxtoolCmd --command 06,8A,00,07,00,00,7E,01,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_MON_SYS_UART1: set UBX-MON-SYS message rate
$ubxtoolCmd --command 06,8A,00,07,00,00,9E,06,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_SEC_SIG_UART1: set UBX-SEC-SIG message rate
$ubxtoolCmd --command 06,8A,00,07,00,00,35,06,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_NAV_STATUS_UART1: set UBX-NAV-STATUS message rate
$ubxtoolCmd --command 06,8A,00,07,00,00,1B,00,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-TP-TIMEGRID_TP1: steer to GPS time rather than UTC time
$ubxtoolCmd -z CFG-TP-TIMEGRID_TP1,1 $address:$port | grep ACK;
sleep 1

#CFG-TP-PERIOD_TP1: Time pulse period in microseconds
$ubxtoolCmd -z CFG-TP-PERIOD_TP1,25000 $address:$port | grep ACK;
sleep 1

#CFG-TP-PERIOD_LOCK_TP1: Time pulse period in microseconds
$ubxtoolCmd -z CFG-TP-PERIOD_LOCK_TP1,25000 $address:$port | grep ACK;
sleep 1

#CFG-TP-LEN_TP1: Length of pulse in microseconds when no GNSS lock
$ubxtoolCmd -z CFG-TP-LEN_TP1,2500 $address:$port | grep ACK;
sleep 1

#CFG-TP-LEN_LOCK_TP1: Length of pulse in microseconds when no GNSS lock
$ubxtoolCmd -z CFG-TP-LEN_LOCK_TP1,2500 $address:$port | grep ACK;
sleep 1


# EXISTING MESSAGES

#CFG-RATE-MEAS: set measurement rate to 10hz
$ubxtoolCmd --command 06,8A,00,07,00,00,01,00,21,30,64,00 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_RXM_MEASX_UART1: set RAWX frames output at 1hz
$ubxtoolCmd --command 06,8A,00,07,00,00,05,02,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-NMEA_ID_GGA_UART1: Set NMEA message output to UART1
$ubxtoolCmd --command 06,8A,00,07,00,00,BB,00,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_MON_SPAN_UART1: set MON-SPAN message output at 1hz
$ubxtoolCmd --command 06,8A,00,07,00,00,8C,03,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-ITFM-ANTSETTING: Force to use passive antenna instead of UNKNOWN
$ubxtoolCmd --command 06,8A,00,07,00,00,10,00,41,20,01 $address:$port | grep ACK;
sleep 1

#CFG-MSGOUT-UBX_NAV_SIG_UART1: set NAV-SIG message output at 1hz
$ubxtoolCmd --command 06,8A,00,07,00,00,46,03,91,20,01 $address:$port | grep ACK;
sleep 1

#CFG-RXM-RAWX: set RXM-RAWX output over UART at 1 hz
$ubxtoolCmd --command 06,8A,00,07,00,00,A5,02,91,20,01 $address:$port | grep ACK;
sleep 1

#UNDOCUMENTED MESSAGE: Ovveride unhealthy status of L5 to use those messages
$ubxtoolCmd --command 06,8A,00,01,00,00,01,00,32,10,01 $address:$port | grep ACK;
sleep 1


$ubxtoolCmd -z CFG-NAVSPG-DYNMODEL,4 $address:$port | grep ACK;
sleep 1
$ubxtoolCmd -z CFG-NAVSPG-SIGATTCOMP,$sigAttn $address:$port | grep ACK;
sleep 1
$ubxtoolCmd -z CFG-NAVSPG-INIFIX3D,1 $address:$port | grep ACK;
sleep 1
$ubxtoolCmd -z CFG-NAVSPG-INFIL_MINCNO,$sigMinCNo $address:$port | grep ACK;
sleep 1
$ubxtoolCmd -z CFG-NAVSPG-INFIL_MINELEV,0 $address:$port | grep ACK;
sleep 1
$ubxtoolCmd -z CFG-SBAS-PRNSCANMASK,0 $address:$port | grep ACK;
sleep 1

$ubxtoolCmd -p SAVE $address:$port | grep ACK;

#cat /sys/firmware/devicetree/base/serial-number
