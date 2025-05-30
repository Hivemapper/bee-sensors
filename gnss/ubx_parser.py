""" U-blox messages data parser """

__author__ = "D. Knowles"
__date__ = "02 Oct 2024"

import os
import csv
import argparse

import gnss_lib_py as glp
from pyubx2 import UBXReader, UBX_PROTOCOL


class UbxParser():

    def __init__(self, input_path):
        """Parse all UBX messages from file and write to csv.

        Parameters
        ----------
        input_path : string
            Path to UBX file

        """

        self.input_path = input_path      # path to UBX input file
        self.ubx_csv_files = dict()       # dictionary of csv file paths for each message type

        self.conversions = self._ubx_name_conversions()

        self.save_ubx_msgs_to_csv()
    

    def save_ubx_msgs_to_csv(self):

        with open(self.input_path, 'rb') as stream:
            ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
            
            epoch_gps_millis = None # timestamp of epoch
            epoch_csv_data = {}     # data for epoch

            for raw_data, parsed_data in ubr:

                # if end of epoch, then write all epoch data to CSV if a valid gpstime was found
                if parsed_data.identity == "NAV-EOE":
                    # only write if epoch timestamp is valid
                    if epoch_gps_millis is not None:
                        self.write_data_to_csv(epoch_csv_data, epoch_gps_millis)

                    # reset epoch data
                    epoch_gps_millis = None
                    epoch_csv_data = {}
                    continue

                if parsed_data.identity == "NAV-TIMEGPS":
                    epoch_gps_millis = self.get_gps_millis_from_gpstime(parsed_data)

                # # convert name to CamelCase
                # new_identity = "Ubx" + "".join([w.capitalize() for w in parsed_data.identity.split("-")])
                # print(new_identity)
                msg_metadata = dict()       # message data that's the same for whole message
                msg_per_sv_data = dict()    # message data that changes for each satellite
                msg_per_sv_labels = []      # unique labels for per satellite data

                for name, value in parsed_data.__dict__.items():
                    if name[0] == "_" or "reserved" in name or "CFG" in name:
                        # ignore private and reserved attributes
                        continue
                    if "_" not in name:
                        # save metadata
                        if name in self.conversions and self.conversions[name][1] is not None:
                            value = self.conversions[name][1][value]
                        msg_metadata[self.conversions.get(name,[name])[0]] = value
                    else:
                        temp_name = name.split("_")[0]
                        temp_name = self.conversions.get(temp_name,[temp_name])[0]

                        # save per-sv data
                        idx = int(name.split("_")[1])
                        if idx not in msg_per_sv_data:
                            msg_per_sv_data[idx] = dict()
                        
                        if name.split("_")[0] in self.conversions and self.conversions[name.split("_")[0]][1] is not None:
                            value = self.conversions[name.split("_")[0]][1][value]
                        msg_per_sv_data[idx][temp_name] = value

                        # add to unique labels if not already there
                        if temp_name not in msg_per_sv_labels:
                            msg_per_sv_labels.append(temp_name)

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
                if parsed_data.identity in epoch_csv_data and parsed_data.identity.split("-")[0] == "NAV":
                    print("Warning: duplicate",parsed_data.identity,"identity found in epoch")
                else:
                    epoch_csv_data[parsed_data.identity] = (labels, csv_data)

    def write_data_to_csv(self, epoch_csv_data, epoch_gps_millis):
        """Write epoch data to csv files.

        Parameters
        ----------
        epoch_csv_data : dict
            Dictionary of parsed UBX messages for an epoch.
        epoch_gps_millis : float
            GPS time in milliseconds for the epoch.
        ubx_csv_files : dict
            Dictionary of csv file paths for each message type.

        """
        # write to csv
        for identity, (labels, csv_data) in epoch_csv_data.items():

            if len(self.ubx_csv_files) == 0 or identity not in self.ubx_csv_files:
                dir_name = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                        "results",os.path.basename(self.input_path).split(".")[0])
                if len(self.ubx_csv_files) == 0:
                    # make results directory if it doesn't exist in root of bee-sensors directory
                    os.makedirs(dir_name, exist_ok=True)

                # write labels to csv
                if identity not in self.ubx_csv_files:
                    self.ubx_csv_files[identity] = os.path.join(dir_name, identity.replace("-","_") + ".csv")
                    with open(self.ubx_csv_files[identity], 'w') as f:
                        writer = csv.writer(f)
                        if identity.split("-")[0] == "RXM":
                            # don't use gps_millis (of NAV solution) for RXM messages
                            writer.writerow(labels)
                        else:
                            writer.writerow(["gps_millis"] + labels)

            if identity.split("-")[0] != "RXM":
                # add gps_millis to each row of data
                csv_data = [[epoch_gps_millis] \
                            + row for row in csv_data]

            # write data to csv files
            with open(self.ubx_csv_files[identity], 'a') as f:
                writer = csv.writer(f)
                writer.writerows(csv_data)

    def get_gps_millis_from_gpstime(self, parsed_data):
        """Get gps_millis from gpstime message

        Parameters
        ----------
        parsed_data : dict
            Dictionary containing GPS time information for a given time step.

        Returns
        -------
        gps_millis : float
            GPS time in milliseconds.

        """

        gps_millis = None
        if "towValid" not in parsed_data.__dict__ or "weekValid" not in parsed_data.__dict__:
            # if gps time is not valid, then return None
            return gps_millis
        if parsed_data.towValid and parsed_data.weekValid:

            gps_week = parsed_data.week
            gps_tow = parsed_data.iTOW * 1E-3 + parsed_data.fTOW * 1E-9
            gps_millis = glp.tow_to_gps_millis(gps_week, gps_tow)

        return gps_millis
    
    def _ubx_name_conversions(self):
        """Conver between UBX names and glp/readable names.

        Returns
        -------
        conversions : dict
            Dictionary of UBX names to glp/readable names.
        
        """

        conversions = dict()

        gnss_id_conversions = {
            0: "gps",
            1: "sbas",
            2: "galileo",
            3: "beidou",
            4: "imes",
            5: "qzss",
            6: "glonass",
            7: "irnss",            
        }
        conversions["gnssId"] = ("gnss_id", gnss_id_conversions)
        conversions["svId"] = ("sv_id", None)
        conversions["cno"] = ("cn0_dbhz", None)
        conversions["elev"] = ("el_sv_deg", None)
        conversions["azim"] = ("az_sv_deg", None)
        conversions["prRes"] = ("residuals_m", None)
        conversions["lon"] = ("lon_rx_deg", None)
        conversions["lat"] = ("lat_rx_deg", None)
        conversions["height"] = ("alt_rx_m", None)

        return conversions



def setup_parser():
  """Parse command line arguments.
  
  """
  parser = argparse.ArgumentParser(description='Parse ubx file')
  parser.add_argument('-i','--input', type=str, default="", help="UBX file to parse")
  return parser.parse_args()

if __name__ == '__main__':
  parser = setup_parser()
  if parser.input != "":
    UbxParser(parser.input)