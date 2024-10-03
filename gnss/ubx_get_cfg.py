"""Save u-blox cfguration parameters to file.

"""

__authors__ = "D. Knowles"
__date__ = "27 Sep 2024"

import csv
import time
import argparse
import subprocess

CFG_GROUPS = [
             # "CFG-BDS",       # not on our ublox chip
             "CFG-HW",
             "CFG-I2C",
             "CFG-I2CINPROT",
             "CFG-I2COUTPROT",
             "CFG-INFMSG",
             "CFG-MOT",
             "CFG-MSGOUT",
             # "CFG-NAV2",        # not on our ublox chip
             # "CFG-NAVMASK",     # not on our ublox chip
             "CFG-NAVSPG",
             "CFG-NMEA",
             "CFG-RATE",
             "CFG-RINV",
             "CFG-SBAS",
             "CFG-SEC",
             "CFG-SIGNAL",
             "CFG-SPI",
             "CFG-SPIINPROT",
             "CFG-SPIOUTPROT",
             # "CFG-TMODE",       # not on our ublox chip
             "CFG-TP",
             "CFG-TXREADY",
             "CFG-UART1",
             "CFG-UART1INPROT",
             "CFG-UART1OUTPROT",
             ]

# conversions from our ublox default names to names that match the
# interface document
CONVERSION_F10_TO_INTERFACE = {
    "CFG-246-10":"CFG-SEC-CFG_LOCK_UNLOCKGRP1",
    "CFG-246-11":"CFG-SEC-CFG_LOCK_UNLOCKGRP2",
    "CFG-54-7":"CFG-SBAS-USE_IONOONLY",
    "CFG-54-8":"CFG-SBAS-ACCEPT_NOT_IN_PRNMASK",
    "CFG-MSGOUT-132":"CFG-MSGOUT-UBX_NAV_COV_UART1",
    "CFG-MSGOUT-135":"CFG-MSGOUT-UBX_NAV_COV_SPI",
    "CFG-MSGOUT-137":"CFG-MSGOUT-UBX_SEC_SIGLOG_I2C",
    "CFG-MSGOUT-138":"CFG-MSGOUT-UBX_SEC_SIGLOG_UART1",
    "CFG-MSGOUT-141":"CFG-MSGOUT-UBX_SEC_SIGLOG_SPI",
    "CFG-MSGOUT-157":"CFG-MSGOUT-UBX_MON_SYS_I2C",
    "CFG-MSGOUT-158":"CFG-MSGOUT-UBX_MON_SYS_UART1",
    "CFG-MSGOUT-161":"CFG-MSGOUT-UBX_MON_SYS_SPI",
    "CFG-MSGOUT-162":"CFG-MSGOUT-UBX_NAV_TIMENAVIC_I2C",
    "CFG-MSGOUT-163":"CFG-MSGOUT-UBX_NAV_TIMENAVIC_UART1",
    "CFG-MSGOUT-166":"CFG-MSGOUT-UBX_NAV_TIMENAVIC_SPI",
    "CFG-MSGOUT-52":"CFG-MSGOUT-UBX_SEC_SIG_I2C",
    "CFG-MSGOUT-53":"CFG-MSGOUT-UBX_SEC_SIG_UART1",
    "CFG-MSGOUT-56":"CFG-MSGOUT-UBX_SEC_SIG_SPI",
    "CFG-MSGOUT-UBX_NAV_TIMEQZSS_USB":"CFG-MSGOUT-UBX_NAV_TIMEQZSS_UART1",
    "CFG-NMEA-24":"CFG-NMEA-FILT_NAVIC",
    "CFG-SIGNAL-15":"CFG-SIGNAL-BDS_B1C_ENA",
    "CFG-SIGNAL-23":"CFG-SIGNAL-QZSS_L5_ENA",
    "CFG-SIGNAL-29":"CFG-SIGNAL-NAVIC_L5_ENA",
    "CFG-SIGNAL-38":"CFG-SIGNAL-NAVIC_ENA",
    "CFG-SIGNAL-4":"CFG-SIGNAL-GPS_L5_ENA",
    "CFG-SIGNAL-40":"CFG-SIGNAL-BDS_B2A_ENA",
    "CFG-SIGNAL-9":"CFG-SIGNAL-GAL_E5A_ENA",
    "CFG-TP-53":"CFG-TP-DRSTR_TP1",
                              }

def main(ubxtool_cmd, dashcam_address, name):
    """Main run file which gets all CFG parameters and writes to file.

    """

    all_data = []
    for cfg_group in CFG_GROUPS:
        output = run_ubxtool(ubxtool_cmd + ' -g '+cfg_group+',0,0,1200 '\
                          + dashcam_address)

        data = add_cfg_data(output,cfg_group)
        print("read", len(data),"params in CFG group",cfg_group)
        all_data += data

    write_to_file(all_data, name)

def get_timestamp():
    """Get single timestamp for output file.

    Returns
    -------
    timestamp : string
        Current timestamp when file is run as a string.

    """
    timestamp = time.strftime("%Y%m%d%H%M%S")
    return timestamp

def run_ubxtool(command):
    """ Run a ubxtool command by passing in the

    Parameters
    ----------
    command : string
        ubxtool command to execute

    Returns
    -------
    output_list : list
        List of string output that's split by double newlines. Generally
        this equates to different UBX messages.

    """
    command = command.split(' ')
    output = subprocess.run(command,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    output_list = output.split('\n\n')
    return output_list

def add_cfg_data(output_list,cfg_group):
    ""
    new_cfg_data = []

    for msg_out in output_list:
        # filter out everything besides the VALGET response msgs
        if "UBX-CFG-VALGET:" in msg_out:
            # print(msg_out)
            cfg_layer = None
            for line in msg_out.split('\n'):
                # get cfguration layer name which is in parentheses
                if '(' in line and ')' in line:
                    cfg_layer = line.split('(')[-1].split(')')[0]
                    continue
                if "item" in line:
                    words = line.lstrip().split(' ')
                    cfg_name,cfg_register = words[1].split('/')
                    if cfg_name in CONVERSION_F10_TO_INTERFACE:
                        cfg_name = CONVERSION_F10_TO_INTERFACE[cfg_name]
                    cfg_val = words[-1]

                    new_cfg_data.append({"cfg_layer":cfg_layer,
                                         "cfg_group":cfg_group,
                                         "cfg_name":cfg_name,
                                         "cfg_register":cfg_register,
                                         "cfg_val":cfg_val,
                                         })
    return new_cfg_data

def write_to_file(data, name="dashcam"):
    """ Write all data to file.

    Parameters
    ----------
    data : list
        List of configuration parameters to write to file
    name : string
        Name identifier to append to filename.

    """
    filename = "ubx_cfg_" + name + "_" + get_timestamp() + ".csv"
    with open(filename,'w',newline='') as csvfile:
        fieldnames = ['cfg_layer','cfg_group','cfg_name','cfg_register','cfg_val']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def setup_parser():
    """Extract command line arguments.

    Returns
    -------
    cmd_args : list
        List of all command line arguments.

    """
    parser = argparse.ArgumentParser(description='Download all ublox CFG params')
    parser.add_argument("-u","--ublox", type=str, default="", help="path to ubxtool executeable")
    parser.add_argument("-d","--dashcam", type=str, default="", help="dashcam IP address:port")
    parser.add_argument("-n","--name", type=str, default="", help="dashcam name to append to file")
    cmd_args = parser.parse_args()

    return cmd_args


if __name__ == "__main__":
    parser = setup_parser()
    if parser.ublox == "":
        # parser.ublox="/home/hellbender/GNSS/gpsd/gpsd-3.25.1~dev/clients/ubxtool"
        parser.ublox="/home/derek/gnss/gpsd-3.25/gpsd-3.25/clients/ubxtool"
    if parser.name == "":
        parser.name = "dashcam"
    if parser.dashcam == "":
        parser.dashcam = "192.168.0.10:9090"

    main(parser.ublox, parser.dashcam, parser.name)
