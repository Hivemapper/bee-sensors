""" U-blox messages data parser """

__author__ = "D. Knowles"
__date__ = "02 Oct 2024"

import csv
import argparse

import gnss_lib_py as glp
from pyubx2 import UBXReader, UBX_PROTOCOL

def ubx_parse_all_messages(input_path):
    """ Parse all UBX messages from file

    Parameters
    ----------
    input_path : string
        Path to UBX file

    Returns
    -------
    ubx_data : dict
        Dictionary of parsed UBX messages
    """

    ubx_csv_files = dict()

    with open(input_path, 'rb') as stream:
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        
        epoch_gps_millis = None # timestamp of epoch
        epoch_csv_data = {}     # data for epoch

        for raw_data, parsed_data in ubr:

            # if end of epoch, then write all epoch data to CSV if a valid gpstime was found
            if parsed_data.identity == "NAV-EOE":
                # only write if epoch timestamp is valid
                if epoch_gps_millis is not None:
                    # write to csv
                    for identity, (labels, csv_data) in epoch_csv_data.items():

                        # write labels to csv
                        if identity not in ubx_csv_files:
                            ubx_csv_files[identity] = input_path.split(".")[0] + "_" \
                                                                + identity.replace("-","_") + ".csv"
                            with open(ubx_csv_files[identity], 'w') as f:
                                writer = csv.writer(f)
                                writer.writerow(["gps_millis","utc_timestamp"] + labels)

                        # add gps_millis to each row of data
                        csv_data = [[epoch_gps_millis,
                                     glp.gps_millis_to_datetime(epoch_gps_millis)] \
                                    + row for row in csv_data]

                        # write data to csv files
                        with open(ubx_csv_files[identity], 'a') as f:
                            writer = csv.writer(f)
                            writer.writerows(csv_data)

                # reset epoch data
                epoch_gps_millis = None
                epoch_csv_data = {}
                continue

            if parsed_data.identity == "NAV-TIMEGPS":
                epoch_gps_millis = get_gps_millis_from_gpstime(parsed_data)

            # # convert name to CamelCase
            # new_identity = "Ubx" + "".join([w.capitalize() for w in parsed_data.identity.split("-")])
            # print(new_identity)
            msg_metadata = dict()       # message data that's the same for whole message
            msg_per_sv_data = dict()    # message data that changes for each satellite
            msg_per_sv_labels = []      # unique labels for per satellite data

            for name, value in parsed_data.__dict__.items():
                if name[0] == "_" or "reserved" in name:
                   # ignore private and reserved attributes
                   continue
                if "_" not in name:
                    # save metadata
                    msg_metadata[name] = value
                else:
                    # save per-sv data
                    idx = int(name.split("_")[1])
                    if idx not in msg_per_sv_data:
                        msg_per_sv_data[idx] = dict()
                    msg_per_sv_data[idx][name.split("_")[0]] = value

                    if name.split("_")[0] not in msg_per_sv_labels:
                        # add to unique labels if not already there
                        msg_per_sv_labels.append(name.split("_")[0])

            # merge metadata and per-sv data
            csv_data = []
            labels = list(msg_metadata.keys()) + msg_per_sv_labels
            
            if len(msg_per_sv_data) == 0:
                # no satellite data, just write metadata
                row = list(msg_metadata.values())
                csv_data.append(row)
            else:
                for sv_idx in sorted(msg_per_sv_data.keys()):
                    row = list(msg_metadata.values())
                    row += [msg_per_sv_data[sv_idx].get(label, None) for label in msg_per_sv_labels]    
                    csv_data.append(row)

            # add message data to epoch data
            if parsed_data.identity not in epoch_csv_data:
                epoch_csv_data[parsed_data.identity] = (labels, csv_data)
            else:
                print("Warning: duplicate",parsed_data.identity,"identity found in epoch")

def get_gps_millis_from_gpstime(parsed_data):
    """Get gps_millis from gpstime message

    Parameters
    ----------
    parsed_data : dict
        Dictionary containing GPS time information for a given time step.
    itow_to_gps_millis : dict
        Dictionary containing the conversion between iTOW and GPS milliseconds.

    Returns
    -------
    gps_millis : float
        GPS time in milliseconds.

    """

    gps_millis = None
    if parsed_data.towValid and parsed_data.weekValid:

        gps_week = parsed_data.week
        gps_tow = parsed_data.iTOW * 1E-3 + parsed_data.fTOW * 1E-9
        gps_millis = glp.tow_to_gps_millis(gps_week, gps_tow)

    return gps_millis            

def setup_parser():
  parser = argparse.ArgumentParser(description='Parse ubx file')
  parser.add_argument('-i','--input', type=str, default="", help="UBX file to parse")
  return parser.parse_args()

if __name__ == '__main__':
  parser = setup_parser()
  if parser.input != "":
    ubx_parse_all_messages(parser.input)