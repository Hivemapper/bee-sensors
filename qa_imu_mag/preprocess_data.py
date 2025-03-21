"""Input calibration arm IMU/Magnetometer data and output downsampled versions.

"""

__author__ = "D. Knowles"
__date__ = "20 Mar 2025"

import os
import subprocess
from datetime import datetime

import sqlite3
import numpy as np
import pandas as pd

class AverageTest():

    def __init__(self, dataset_dir):
        self.dataset_dir = dataset_dir
        self.min_length_imu = np.inf
        self.min_length_mag = np.inf
        self.imu_logs = {
                         "Station1": {},
                         "Station2": {},
                         "Station3": {},
                        }
        self.mag_logs = {
                         "Station1": {},
                         "Station2": {},
                         "Station3": {},
                        }
        self.device_dir_order = {
                         "Station1": [],
                         "Station2": [],
                         "Station3": [],
                        }
        self.downsampled_data = {
                                   "Station1": {
                                    "acc_x" : [],
                                    "acc_y" : [],
                                    "acc_z" : [],
                                    "gyro_x" : [],
                                    "gyro_y" : [],
                                    "gyro_z" : [],
                                    "mag_x" : [],
                                    "mag_y" : [],
                                    "mag_z" : [],
                                   },
                                    "Station2": {
                                    "acc_x" : [],
                                    "acc_y" : [],
                                    "acc_z" : [],
                                    "gyro_x" : [],
                                    "gyro_y" : [],
                                    "gyro_z" : [],
                                    "mag_x" : [],
                                    "mag_y" : [],
                                    "mag_z" : [],
                                    },
                                    "Station3": {
                                    "acc_x" : [],
                                    "acc_y" : [],
                                    "acc_z" : [],
                                    "gyro_x" : [],
                                    "gyro_y" : [],
                                    "gyro_z" : [],
                                    "mag_x" : [],
                                    "mag_y" : [],
                                    "mag_z" : [],
                                    },
        }
        self.load_dataset()

    def load_dataset(self):
        for station in sorted(os.listdir(self.dataset_dir)):
            print(station)
            directories = [d for d in os.listdir(os.path.join(self.dataset_dir, station)) if os.path.isdir(os.path.join(self.dataset_dir, station, d))]
            for d_idx, device_dir in enumerate(sorted(directories, key=self.extract_timestamp)):
                log_path = os.path.join(self.dataset_dir, station, device_dir, "sensors-v0-0-2.db")
                print(f"Processing {log_path}")
                self.parse_database(log_path, station, device_dir)
                self.device_dir_order[station].append(device_dir)
            #     if d_idx > 9:
            #         break
            # break

        for station in self.imu_logs.keys():
            for device_dir in self.device_dir_order[station]:
                self.imu_logs[station][device_dir] = self.imu_logs[station][device_dir][:self.min_length_imu]
                self.mag_logs[station][device_dir] = self.mag_logs[station][device_dir][:self.min_length_mag]
                self.imu_logs[station][device_dir]["time"] = pd.to_datetime(self.imu_logs[station][device_dir]["time"], format="mixed")
                self.mag_logs[station][device_dir]["system_time"] = pd.to_datetime(self.mag_logs[station][device_dir]["system_time"], format="mixed")
                # change time column to delta that starts at zero
                self.imu_logs[station][device_dir]["time"] = self.imu_logs[station][device_dir]["time"] - self.imu_logs[station][device_dir]["time"].iloc[0]
                self.imu_logs[station][device_dir]["time"] = self.imu_logs[station][device_dir]["time"].dt.total_seconds()
                self.mag_logs[station][device_dir]["system_time"] = self.mag_logs[station][device_dir]["system_time"] - self.mag_logs[station][device_dir]["system_time"].iloc[0]
                self.mag_logs[station][device_dir]["system_time"] = self.mag_logs[station][device_dir]["system_time"].dt.total_seconds()
                # move to zero mean for each column
                self.imu_logs[station][device_dir]["acc_x"] = self.imu_logs[station][device_dir]["acc_x"] - self.imu_logs[station][device_dir]["acc_x"].iloc[-2000:].mean()
                self.imu_logs[station][device_dir]["acc_y"] = self.imu_logs[station][device_dir]["acc_y"] - self.imu_logs[station][device_dir]["acc_y"].iloc[-2000:].mean()
                self.imu_logs[station][device_dir]["acc_z"] = self.imu_logs[station][device_dir]["acc_z"] - self.imu_logs[station][device_dir]["acc_z"].iloc[-2000:].mean()
                self.imu_logs[station][device_dir]["gyro_x"] = self.imu_logs[station][device_dir]["gyro_x"] - self.imu_logs[station][device_dir]["gyro_x"].iloc[-2000:].mean()
                self.imu_logs[station][device_dir]["gyro_y"] = self.imu_logs[station][device_dir]["gyro_y"] - self.imu_logs[station][device_dir]["gyro_y"].iloc[-2000:].mean()
                self.imu_logs[station][device_dir]["gyro_z"] = self.imu_logs[station][device_dir]["gyro_z"] - self.imu_logs[station][device_dir]["gyro_z"].iloc[-2000:].mean()
                self.mag_logs[station][device_dir]["mag_x"] = self.mag_logs[station][device_dir]["mag_x"] - self.mag_logs[station][device_dir]["mag_x"].iloc[-250:].mean()
                self.mag_logs[station][device_dir]["mag_y"] = self.mag_logs[station][device_dir]["mag_y"] - self.mag_logs[station][device_dir]["mag_y"].iloc[-250:].mean()
                self.mag_logs[station][device_dir]["mag_z"] = self.mag_logs[station][device_dir]["mag_z"] - self.mag_logs[station][device_dir]["mag_z"].iloc[-250:].mean()
                # remove session column
                self.imu_logs[station][device_dir] = self.imu_logs[station][device_dir].drop(columns=["session"])
                self.mag_logs[station][device_dir] = self.mag_logs[station][device_dir].drop(columns=["session"])

                for col in ["gyro_x", "gyro_y", "gyro_z", "acc_x", "acc_y", "acc_z", "mag_x", "mag_y", "mag_z"]:
                    self.downsampled_data[station][col].append(self.downsample(station, device_dir, col))

                # for col in ["gyro_x", "gyro_y", "gyro_z", "acc_x", "acc_y", "acc_z"]:
                #     fig, ax = plt.subplots(5,1, figsize=(18, 10))
                #     original = self.imu_logs[station][device_dir][col].to_numpy()
                #     ax[0].plot(np.arange(len(original)),original)
                #     ax[0].set_title(f"{col} original")
                #     downsampled_50_mean = self.imu_logs[station][device_dir].groupby(self.imu_logs[station][device_dir].index // 50).mean()[col].to_numpy()
                #     ax[1].plot(np.arange(len(downsampled_50_mean)),downsampled_50_mean)
                #     ax[1].set_title(f"{col} downsampled 50 mean")
                #     downsampled_50 = self.imu_logs[station][device_dir][col].to_numpy()[::50]
                #     ax[2].plot(np.arange(len(downsampled_50)),downsampled_50)
                #     ax[2].set_title(f"{col} Downsampled 50")
                #     smoothed = self.imu_logs[station][device_dir][col].rolling(50).mean().to_numpy()
                #     ax[3].plot(np.arange(len(smoothed)),smoothed)
                #     ax[3].set_title(f"{col} Smoothed 50")
                #     smoothed_downsampled = self.imu_logs[station][device_dir][col].rolling(200).mean().to_numpy()[::50]
                #     ax[4].plot(np.arange(len(smoothed_downsampled)),smoothed_downsampled)
                #     ax[4].set_title(f"{col} Smoothed 200 Downsampled 50")
                # for col in ["mag_x", "mag_y", "mag_z"]:
                #     fig, ax = plt.subplots(5,1, figsize=(18, 10))
                #     original = self.mag_logs[station][device_dir][col].to_numpy()
                #     ax[0].plot(np.arange(len(original)),original)
                #     ax[0].set_title(f"{col} original")
                #     downsampled_50_mean = self.mag_logs[station][device_dir].groupby(self.mag_logs[station][device_dir].index // 25).mean()[col].to_numpy()
                #     ax[1].plot(np.arange(len(downsampled_50_mean)),downsampled_50_mean)
                #     ax[1].set_title(f"{col} downsampled 50 mean")
                #     downsampled_50 = self.mag_logs[station][device_dir][col].to_numpy()[::8]
                #     ax[2].plot(np.arange(len(downsampled_50)),downsampled_50)
                #     ax[2].set_title(f"{col} Downsampled 50")
                #     smoothed = self.mag_logs[station][device_dir][col].rolling(25).mean().to_numpy()
                #     ax[3].plot(np.arange(len(smoothed)),smoothed)
                #     ax[3].set_title(f"{col} Smoothed 50")
                #     smoothed_downsampled = self.mag_logs[station][device_dir][col].rolling(25).mean().to_numpy()[::8]
                #     ax[4].plot(np.arange(len(smoothed_downsampled)),smoothed_downsampled)
                #     ax[4].set_title(f"{col} Smoothed 200 Downsampled 50")


        # save each station data type to csv
        for station in self.downsampled_data.keys():
            for col in ["gyro_x", "gyro_y", "gyro_z", "acc_x", "acc_y", "acc_z", "mag_x", "mag_y", "mag_z"]:
                barycenter_data = np.array(self.downsampled_data[station][col]).T
                print(barycenter_data.shape)
                df = pd.DataFrame(barycenter_data, columns=self.device_dir_order[station])
                sensor_path = os.path.join(self.dataset_dir, station, col + ".csv")
                df.to_csv(sensor_path, index=False)

    def downsample(self, station, device_dir, col):
        if col in ["mag_x", "mag_y", "mag_z"]:
            downsampled = self.mag_logs[station][device_dir][col].rolling(25).mean().to_numpy()[::8]
        else:
            downsampled = self.imu_logs[station][device_dir][col].rolling(200).mean().to_numpy()[::50]
        downsampled = downsampled[~np.isnan(downsampled)]
        return downsampled

    def extract_timestamp(self, directory_name):
        timestamp_str = directory_name.rsplit("_", 1)[-1]
        return datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

    def parse_database(self, db_path, station, device_dir):
        """Parse sqlite3 database file.

        Use sqlite3 to connect then read from pandas.

        Attempt recovery if database file is corrupted (requires sqlite3 installed on device).

        Parameters
        ----------
        db_path : str
            Path to the database file.

        Returns
        -------
        logs : dict
            Dictionary containing the dataframes for each sensor.
        
        """

        if not os.path.isfile(db_path):
            print(f"no such file {db_path} found.")
            self.mag_logs[station][device_dir] = None
            self.imu_logs[station][device_dir] = None
            return

        # Connect to the database
        conn = sqlite3.connect(db_path)

        # add imu data
        try:
            df = pd.read_sql_query("SELECT * FROM imu", conn)
            self.imu_logs[station][device_dir] = df
        except Exception as e:
            print(f"imu db error: {e}")
            logger_recovered_path = db_path[:-3] + "_recovered.db"
            if not os.path.isfile(logger_recovered_path):
                # recover file
                print("attempting recovery")
                self.recover_sqlite_db(db_path,logger_recovered_path)
            else:
                print("using previously recovered db file")
            conn = sqlite3.connect(logger_recovered_path)

            try:
                df = pd.read_sql_query("SELECT * FROM imu", conn)
                self.imu_logs[station][device_dir] = df
            except Exception as e:
                print(f"imu db error: {e}")
                self.imu_logs[station][device_dir] = None

        # add magnetometer data
        try:
            df = pd.read_sql_query("SELECT * FROM magnetometer", conn)
            self.mag_logs[station][device_dir] = df
        except Exception as e:
            print(f"magnetometer db error: {e}")
            self.mag_logs[station][device_dir] = None
            
        # Close the connection
        conn.close()

        self.min_length_imu = min(self.min_length_imu, len(self.imu_logs[station][device_dir]))
        self.min_length_mag = min(self.min_length_mag, len(self.mag_logs[station][device_dir]))

        return

    def recover_sqlite_db(self,corrupt_db_path, recovered_db_path):
        """
        Recovers a corrupt SQLite database using the .recover command.

        Parameters
        ----------
        corrupt_db_path : str
            Path to the corrupt database file.
        recovered_db_path : str
            Path to the recovered database file.

        """

        try:
            subprocess.run(f"sqlite3 {corrupt_db_path} .recover | sqlite3 {recovered_db_path}", shell=True, check=True)
            print("Database recovered successfully.")
        except subprocess.CalledProcessError as e:
            print("Error during recovery:", e)

if __name__ == "__main__":
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    avg = AverageTest(dataset_path)