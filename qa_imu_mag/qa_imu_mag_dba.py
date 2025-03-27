"""QA test comparing IMU/Mag data against the average for the robot arm.

"""

__author__ = "D. Knowles"
__date__ = "20 Mar 2025"

import os
import subprocess
from datetime import datetime

import sqlite3
import numpy as np
import pandas as pd

class QaImuMagDBA():

    def __init__(self, db_path):
        self.db_path = db_path
        
        self.mag_log = None
        self.imu_log = None
        self.station_reference = None
        self.test_data = None

        self.imu_cols = ["acc_x", "acc_y", "acc_z", "gyro_x", "gyro_y", "gyro_z"]
        self.mag_cols = ["mag_x", "mag_y", "mag_z"]
        self.downsampled_data = {c : None for c in self.imu_cols + self.mag_cols}
        self.test_results = {c : None for c in self.imu_cols + self.mag_cols}
        self.test_metrics = {}

        self.load_database()
        self.load_station_reference()

    def load_database(self):

        self.parse_database(self.db_path)

        self.imu_log["time"] = pd.to_datetime(self.imu_log["time"], format="mixed")
        self.mag_log["system_time"] = pd.to_datetime(self.mag_log["system_time"], format="mixed")
        # change time column to delta that starts at zero
        self.imu_log["time"] = self.imu_log["time"] - self.imu_log["time"].iloc[0]
        self.imu_log["time"] = self.imu_log["time"].dt.total_seconds()
        self.mag_log["system_time"] = self.mag_log["system_time"] - self.mag_log["system_time"].iloc[0]
        self.mag_log["system_time"] = self.mag_log["system_time"].dt.total_seconds()
        # move to zero mean for each column
        self.test_metrics["zero_mean_bias"] = {}
        for col in self.imu_cols:
            zero_mean_bias = self.imu_log[col].iloc[-2000:].mean()
            self.imu_log[col] = self.imu_log[col] - zero_mean_bias
            self.test_metrics["zero_mean_bias"][col] = zero_mean_bias
        for col in self.mag_cols:
            zero_mean_bias = self.mag_log[col].iloc[-250:].mean()
            self.mag_log[col] = self.mag_log[col] - zero_mean_bias
            self.test_metrics["zero_mean_bias"][col] = zero_mean_bias

        # remove session column
        self.imu_log = self.imu_log.drop(columns=["session"])
        self.mag_log = self.mag_log.drop(columns=["session"])

        for col in self.imu_cols + self.mag_cols:
            self.downsampled_data[col] = self.downsample(col)
        
        self.test_data = pd.DataFrame(self.downsampled_data)
        # for col in ["gyro_x", "gyro_y", "gyro_z", "acc_x", "acc_y", "acc_z"]:
        #     fig, ax = plt.subplots(5,1, figsize=(18, 10))
        #     original = self.imu_log[col].to_numpy()
        #     ax[0].plot(np.arange(len(original)),original)
        #     ax[0].set_title(f"{col} original")
        #     downsampled_50_mean = self.imu_log.groupby(self.imu_log.index // 50).mean()[col].to_numpy()
        #     ax[1].plot(np.arange(len(downsampled_50_mean)),downsampled_50_mean)
        #     ax[1].set_title(f"{col} downsampled 50 mean")
        #     downsampled_50 = self.imu_log[col].to_numpy()[::50]
        #     ax[2].plot(np.arange(len(downsampled_50)),downsampled_50)
        #     ax[2].set_title(f"{col} Downsampled 50")
        #     smoothed = self.imu_log[col].rolling(50).mean().to_numpy()
        #     ax[3].plot(np.arange(len(smoothed)),smoothed)
        #     ax[3].set_title(f"{col} Smoothed 50")
        #     smoothed_downsampled = self.imu_log[col].rolling(200).mean().to_numpy()[::50]
        #     ax[4].plot(np.arange(len(smoothed_downsampled)),smoothed_downsampled)
        #     ax[4].set_title(f"{col} Smoothed 200 Downsampled 50")
        # for col in ["mag_x", "mag_y", "mag_z"]:
        #     fig, ax = plt.subplots(5,1, figsize=(18, 10))
        #     original = self.mag_log[col].to_numpy()
        #     ax[0].plot(np.arange(len(original)),original)
        #     ax[0].set_title(f"{col} original")
        #     downsampled_50_mean = self.mag_log.groupby(self.mag_log.index // 25).mean()[col].to_numpy()
        #     ax[1].plot(np.arange(len(downsampled_50_mean)),downsampled_50_mean)
        #     ax[1].set_title(f"{col} downsampled 50 mean")
        #     downsampled_50 = self.mag_log[col].to_numpy()[::8]
        #     ax[2].plot(np.arange(len(downsampled_50)),downsampled_50)
        #     ax[2].set_title(f"{col} Downsampled 50")
        #     smoothed = self.mag_log[col].rolling(25).mean().to_numpy()
        #     ax[3].plot(np.arange(len(smoothed)),smoothed)
        #     ax[3].set_title(f"{col} Smoothed 50")
        #     smoothed_downsampled = self.mag_log[col].rolling(25).mean().to_numpy()[::8]
        #     ax[4].plot(np.arange(len(smoothed_downsampled)),smoothed_downsampled)
        #     ax[4].set_title(f"{col} Smoothed 200 Downsampled 50")

    def load_station_reference(self):
        station = self.db_path.split("/")[-3]
        station_reference_path = os.path.join(os.path.dirname(self.db_path), "..", f"dba_centers_{station.lower()}.csv")
        self.station_reference = pd.read_csv(station_reference_path)

    def downsample(self, col):
        if col in ["mag_x", "mag_y", "mag_z"]:
            downsampled = self.mag_log[col].rolling(25).mean().to_numpy()[::8]
        else:
            downsampled = self.imu_log[col].rolling(200).mean().to_numpy()[::50]
        downsampled = downsampled[~np.isnan(downsampled)]
        return downsampled

    def parse_database(self, db_path):
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
            print(f"[ERROR] no such file {db_path} found.")
            self.mag_log = None
            self.imu_log = None
            return

        # Connect to the database
        conn = sqlite3.connect(db_path)

        # add imu data
        try:
            df = pd.read_sql_query("SELECT * FROM imu", conn)
            self.imu_log = df
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
                self.imu_log = df
            except Exception as e:
                print(f"imu db error: {e}")
                self.imu_log = None

        # add magnetometer data
        try:
            df = pd.read_sql_query("SELECT * FROM magnetometer", conn)
            self.mag_log = df
        except Exception as e:
            print(f"magnetometer db error: {e}")
            self.mag_log = None
            
        # Close the connection
        conn.close()

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

def extract_timestamp(directory_name):
    timestamp_str = directory_name.rsplit("_", 1)[-1]
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

if __name__ == "__main__":
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    for station in sorted(os.listdir(dataset_path)):
            print(station)
            directories = [d for d in os.listdir(os.path.join(dataset_path, station)) if os.path.isdir(os.path.join(dataset_path, station, d))]
            for d_idx, device_dir in enumerate(sorted(directories, key=extract_timestamp)):
                log_dir = os.path.join(dataset_path, station, device_dir)
                log_path = next((os.path.join(log_dir,x) for x in os.listdir(log_dir) if x.endswith(".db") and "sensors" in x), None)
                print(f"Processing {log_path}")
                avg = QaImuMagDBA(log_path)
                if d_idx > 5:
                    break
            break