""" Imu/Mag Automatic QA script.

"""

import os
import time
import json
import sqlite3
import argparse
import subprocess


def geq(ver1, ver2):
    """ Returns true if ver1 >= ver2"""
    x1,y1,z1 = ver1.split(".")
    x2,y2,z2 = ver2.split(".")
    
    if int(x1) > int(x2):
        return True
    if int(x1) < int(x2):
        return False
    if int(y1) > int(y2):
        return True
    if int(y1) < int(y2):
        return False
    if int(z1) > int(z2):
        return True
    if int(z1) < int(z2):
        return False    
    return True

def less_than(ver1, ver2):
    """ Returns true if ver1 < ver2"""
    return not geq(ver1, ver2)


class ImuMagQa():
    def __init__(self, db_path, name="", sn=""):
        self.database_path = db_path
        self.name = name
        self.sn = sn

        self.imu_columns = ["id", "time", "session",
                            "acc_x","acc_y", "acc_z",
                            "gyro_x", "gyro_y", "gyro_z",
                            ]
        self.mag_columns = ["id", "system_time", "session",
                            "mag_x","mag_y", "mag_z",
                            ]
        
        self.checked_imu = False
        self.check_acc_zeros = False
        self.check_gyro_zeros = False
        self.checked_mag = False
        self.check_mag_zeros = False
        
        self.imu_ids_seen = set()
        self.mag_ids_seen = set()

        self.imu_data = {"acc_x": [], "acc_y": [], "acc_z": [],
                         "gyro_x": [], "gyro_y": [], "gyro_z": []}
        self.mag_data = {"mag_x": [], "mag_y": [], "mag_z": []}
        
        self.count = 0
        self.num_rows_to_fetch = 25
        self.secs_to_fetch = 4

    def run(self):

        while True:

            print(f"count: {self.count}")

            if self.count >= 60:
                print("60s Timeout. Exiting...")
                self._write_results()
                break

            latest_imu = self._get_latest_values("imu", self.imu_columns)
            if not latest_imu or len(latest_imu) == 0:
                print("empty imu database.")
                self.count += 1
                time.sleep(1)
                continue
            if len(self.imu_ids_seen.intersection(set(latest_imu["id"]))) > 0:
                print("stale imu data.")
                self.count += 1
                time.sleep(1)
                continue
            self.imu_ids_seen.update(latest_imu["id"])
            for key in self.imu_data.keys():
                self.imu_data[key].extend(latest_imu[key])

            latest_mag = self._get_latest_values("magnetometer", self.mag_columns)
            if not latest_mag or len(latest_mag) == 0:
                print("empty mag database.")
                self.count += 1
                time.sleep(1)
                continue
            if len(self.mag_ids_seen.intersection(set(latest_mag["id"]))) > 0:
                print("stale mag data.")
                self.count += 1
                time.sleep(1)
                continue
            self.mag_ids_seen.update(latest_mag["id"])
            for key in self.mag_data.keys():
                self.mag_data[key].extend(latest_mag[key])

            if len(self.imu_data["acc_x"]) >= self.secs_to_fetch*self.num_rows_to_fetch and not self.checked_imu:
                self.check_acc_zeros = self._check_acc_zeros()
                self.check_gyro_zeros = self._check_gyro_zeros()
                self.checked_imu = True
            if len(self.mag_data["mag_x"]) >= self.secs_to_fetch*self.num_rows_to_fetch and not self.checked_mag:
                self.check_mag_zeros = self._check_mag_zeros()
                self.checked_mag = True

            if self.checked_imu and self.checked_mag:
                break
            
            self.count += 1
            time.sleep(5)

        self._write_results()


    def _check_acc_zeros(self):
        """Check less than 5% of data is zero.

        """
        # check that less than 5% are zeros
        for key in ["acc_x", "acc_y", "acc_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.imu_data[key]])/float(len(self.imu_data[key])) > 0.05:
                return False
        return True
    
    def _check_gyro_zeros(self):
        """Check less than 5% of data is zero.

        """
        # check that less than 5% are zeros
        for key in ["gyro_x", "gyro_y", "gyro_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.imu_data[key]])/float(len(self.imu_data[key])) > 0.05:
                return False
        return True
    
    def _check_mag_zeros(self):
        """Check less than 5% of data is zero.

        """
        # check that less than 5% are zeros
        for key in ["mag_x", "mag_y", "mag_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.mag_data[key]])/float(len(self.mag_data[key])) > 0.05:
                return False
        return True

    def _write_results(self):
        """Write results to txt file /data/qa_imu_mag_results.txt

        """

        filename = "/data/qa_imu_mag_results"
        if self.name != "":
            filename += "_"+self.name
        if self.sn != "":
            filename += "_"+self.sn
        filename += ".txt"

        with open(filename, "w") as f:            
            if self.check_acc_zeros:
                f.write("[PASS] accel values 0.0 for less than 5% of time\n")
            else:
                f.write("[FAIL] accel values 0.0 for greater than 5% of time\n")
            if self.check_gyro_zeros:
                f.write("[PASS] gyro values 0.0 for less than 5% of time\n")
            else:
                f.write("[FAIL] gyro values 0.0 for greater than 5% of time\n")

            if self.check_mag_zeros:
                f.write("[PASS] mag values 0.0 for less than 5% of time\n")
            else:
                f.write("[FAIL] mag values 0.0 for greater than 5% of time\n")

    def _get_latest_values(self, table_name, columns, order_by_column = "id"):
        """
        Fetch the last values from specific columns in a SQLite3 database table.

        :param database_path: Path to the SQLite database file.
        :param table_name: Name of the table to query.
        :param columns: List of column names to retrieve.
        :param order_by_column: Column name used to determine the order (e.g., timestamp or ID).
        :return: A list of dictionaries containing the latest values for the specified columns.
        """
        try:
            # Connect to the database
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()

            # Build the SQL query
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {table_name} ORDER BY {order_by_column} DESC LIMIT {self.num_rows_to_fetch}"

            # Execute the query
            cursor.execute(query)

            # Fetch the results
            results = cursor.fetchall()

            # Check if any results were returned
            if not results:
                print("No data found in the table.")
                return {}

            # Transpose rows into columns
            column_data = {column: [] for column in columns}
            for row in results:
                for col_name, value in zip(columns, row):
                    column_data[col_name].append(value)

            return column_data

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return {}

        finally:
            # Close the connection
            if conn:
                conn.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process device information.")
    parser.add_argument("--name", default="", help="Name of the technician.")
    parser.add_argument("--sn", default="", help="Serial number of the Bee device.")
    args = parser.parse_args()

    # read version from /etc/build_info.json variable
    with open("/etc/build_info.json") as file:
        build_info = json.load(file)
    firmware_version = build_info["odc-version"]

    
    # Move this up top
    from pathlib import Path
    
    # Choose the appropriate database path based on the firmware version
    database_path = None
    if geq(firmware_version, "5.0.19") and less_than(firmware_version, "5.0.26"):
        database_path = Path("/data/redis_handler/redis_handler-v0-0-3.db")
    elif geq(firmware_version, "5.0.26") and less_than(firmware_version, "5.1.4"):
        database_path = Path("/data/recording/redis_handler/redis_handler-v0-0-3.db")
    elif geq(firmware_version, "5.1.4"):
        directory_path = Path("/data/recording/redis_handler/")
        database_path = next(directory_path.glob("*sensors*.db"), None)

    if database_path is None:
        raise Exception("Could not determine the database path for the current firmware version.")
    # FileIO will do the correct thing if passed a Path, but assuming sqlite3.connect requires a str
    # use the str wrapper below.
    qa = ImuMagQa(str(database_path), args.name, args.sn)
    qa.run()