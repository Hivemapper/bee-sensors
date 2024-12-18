"""Save u-blox cfguration parameters to file.

"""

__authors__ = "D. Knowles"
__date__ = "02 Sep 2024"

import csv
import argparse

def main(cfg1_filepath, cfg2_filepath):
    """Main run function.

    Parameters
    ----------
    cfg1_filepath : string
        Path to first configuration csv file.
    cfg2_filepath : string
        Path to second configuration csv file.

    """
    identical = True

    cfg1 = []
    with open(cfg1_filepath) as file_obj:
        reader = csv.reader(file_obj)
        for rr,row in enumerate(reader):
            if rr == 0:
                continue
            else:
                cfg1.append([row[2],row[4],row[0]])
    cfg2 = []
    with open(cfg2_filepath) as file_obj:
        reader = csv.reader(file_obj)
        for rr,row in enumerate(reader):
            if rr == 0:
                continue
            else:
                cfg2.append([row[2],row[4],row[0]])

    # compare with two pointers
    cfg1_idx = 0
    cfg2_idx = 0
    while cfg1_idx < len(cfg1) or cfg2_idx < len(cfg2):

        # check if parameter exists in one file but not the other
        if cfg1_idx == len(cfg1):
            print(cfg2[cfg2_idx][2],cfg2[cfg2_idx][0],"param not in cfg1 file.")
            cfg2_idx += 1
            identical = False
            continue
        elif cfg2_idx == len(cfg2):
            print(cfg1[cfg1_idx][2],cfg1[cfg1_idx][0],"param not in cfg2 file.")
            cfg1_idx += 1
            identical = False
            continue
        elif cfg1[cfg1_idx][0] < cfg2[cfg2_idx][0]:
            print(cfg1[cfg1_idx][2],cfg1[cfg1_idx][0],"param not in cfg2 file.")
            cfg1_idx += 1
            identical = False
            continue
        elif cfg1[cfg1_idx][0] > cfg2[cfg2_idx][0]:
            print(cfg2[cfg2_idx][2],cfg2[cfg2_idx][0],"param not in cfg1 file.")
            cfg2_idx += 1
            identical = False
            continue

        # check if parameters aren't identical
        elif cfg1[cfg1_idx][1] != cfg2[cfg2_idx][1]:
            print(cfg1[cfg1_idx][2],cfg1[cfg1_idx][0],"param not identical")
            print("cfg1 has", cfg1[cfg1_idx][1])
            print("cfg2 has", cfg2[cfg2_idx][1])
            identical = False

        cfg1_idx += 1
        cfg2_idx += 1


    if identical:
        print("cfg1 and cfg2 are identical.")



def setup_parser():
    """Extract command line arguments.

    Returns
    -------
    cmd_args : list
        List of all command line arguments.

    """
    parser = argparse.ArgumentParser(description='Compare two ublox CFG param files')
    parser.add_argument("--cfg1", type=str, default="", help="First ubx cfg file")
    parser.add_argument("--cfg2", type=str, default="", help="Second ubx cfg file")
    cmd_args = parser.parse_args()

    return cmd_args

if __name__ == "__main__":
    parser = setup_parser()

    if parser.cfg1 == "" or parser.cfg2 == "":
        print("Examlpe use: python3 compare_ubx_cfg.py --cfg1 ubx_cfg_active.csv --cfg2 ubx_cfg_passive.csv")
    else:
        main(parser.cfg1, parser.cfg2)
