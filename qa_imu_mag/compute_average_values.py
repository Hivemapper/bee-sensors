"""Check accelerometer, gyroscope, and magnetometer data for quality assurance.

"""

__author__ = "D. Knowles"
__date__ = "24 Oct 2024"

import os
import numbers
import subprocess
from datetime import datetime
from collections import defaultdict

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import interpolate
from scipy.spatial.distance import euclidean

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
        self.imu_logs_mapped = {
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
        self.mapped_imu_lengths = [0,1]
        self.load_dataset()
        self.plot_all_values()

        plt.show()

    def load_dataset(self):
        for station in sorted(os.listdir(self.dataset_dir)):
            print(station)
            for d_idx, device_dir in enumerate(sorted(os.listdir(os.path.join(self.dataset_dir, station)),
                                                      key=self.extract_timestamp)):
                log_path = os.path.join(self.dataset_dir, station, device_dir, "sensors-v0-0-2.db")
                print(f"Processing {log_path}")
                self.parse_database(log_path, station, device_dir)
                self.device_dir_order[station].append(device_dir)
                if d_idx > 2:
                    break
            break

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

        # self.plot_all_values()
        while not np.all(np.array(self.mapped_imu_lengths) == max(self.mapped_imu_lengths)):
            self.mapped_imu_lengths = []
            for station in self.imu_logs.keys():
                for device_dir in self.device_dir_order[station]:
                    # find roll index and roll all imu columns
                    self.compute_dtw(station, device_dir)
                    # roll_idx = self.find_ideal_roll(station, device_dir)
                    # self.imu_logs[station][device_dir]["acc_x"] = np.roll(self.imu_logs[station][device_dir]["acc_x"].to_numpy(), roll_idx)
                    # self.imu_logs[station][device_dir]["acc_y"] = np.roll(self.imu_logs[station][device_dir]["acc_y"].to_numpy(), roll_idx)
                    # self.imu_logs[station][device_dir]["acc_z"] = np.roll(self.imu_logs[station][device_dir]["acc_z"].to_numpy(), roll_idx)
                    # self.imu_logs[station][device_dir]["gyro_x"] = np.roll(self.imu_logs[station][device_dir]["gyro_x"].to_numpy(), roll_idx)
                    # self.imu_logs[station][device_dir]["gyro_y"] = np.roll(self.imu_logs[station][device_dir]["gyro_y"].to_numpy(), roll_idx)
                    # self.imu_logs[station][device_dir]["gyro_z"] = np.roll(self.imu_logs[station][device_dir]["gyro_z"].to_numpy(), roll_idx)

    def find_ideal_roll(self, station, device_dir):
        
        roll_indexes = np.arange(-600, 600)
        l2_diffs = []
        gyro_z_0 = self.imu_logs[station][self.device_dir_order[station][0]]["gyro_z"].to_numpy()
        gyro_z_0 = np.where(np.abs(gyro_z_0) > 5., gyro_z_0, 0.)
        for roll_idx in roll_indexes:
            gyro_z_test = np.roll(self.imu_logs[station][device_dir]["gyro_z"].to_numpy(), roll_idx)
            gyro_z_test = np.where(np.abs(gyro_z_test) > 5., gyro_z_test, 0.)
            l2_diffs.append(np.linalg.norm(gyro_z_0 - gyro_z_test))
        
        roll_idx = roll_indexes[np.argmin(l2_diffs)]
        print("min l2 diff", np.min(l2_diffs), "at idx: ", np.argmin(l2_diffs), "roll_idx", roll_idx, "max l2 diff", np.max(l2_diffs))

    def compute_dtw(self, station, device_dir):
        

        downsample_factor = 50
        if station not in self.imu_logs_mapped.keys() or self.device_dir_order[station][0] not in self.imu_logs_mapped[station].keys():
            self.imu_logs_mapped[station][self.device_dir_order[station][0]] = None
            imu_0 = self.imu_logs[station][self.device_dir_order[station][0]].copy()
            imu_0 = imu_0.groupby(imu_0.index // downsample_factor).mean()
            gyro_z_0 = imu_0["gyro_x"].to_numpy() + imu_0["gyro_y"].to_numpy() + imu_0["gyro_z"].to_numpy()
        else:
            imu_0 = self.imu_logs_mapped[station][self.device_dir_order[station][0]].copy()
            gyro_z_0 = imu_0["gyro_x"].to_numpy() + imu_0["gyro_y"].to_numpy() + imu_0["gyro_z"].to_numpy()
        signal_1 = np.hstack((np.arange(len(gyro_z_0)).reshape(-1,1), gyro_z_0.reshape(-1,1)))

        imu_test = self.imu_logs[station][device_dir].copy()
        imu_test = imu_test.groupby(imu_test.index // downsample_factor).mean()
        gyro_z_test = imu_test["gyro_x"].to_numpy() + imu_test["gyro_y"].to_numpy() + imu_test["gyro_z"].to_numpy()
        signal_2 = np.hstack((np.arange(len(gyro_z_test)).reshape(-1,1), gyro_z_test.reshape(-1,1)))

        # gyro_z_0 = self.imu_logs[station][self.device_dir_order[station][0]]["gyro_z"].to_numpy()[::50]
        # print("gyro_z_0", gyro_z_0.shape)
        # # gyro_z_0_time = self.imu_logs[station][self.device_dir_order[station][0]]["time"].to_numpy()[::10]
        # gyro_z_test = self.imu_logs[station][device_dir]["gyro_z"].to_numpy()[::10]
        # gyro_z_test_time = self.imu_logs[station][self.device_dir_order[station][0]]["time"].to_numpy()[::10]
        distance, path = fastdtw(signal_1, signal_2, radius=50, dist=euclidean)

        new_gyro_z_0 = np.array([gyro_z_0[min(p[0],len(imu_0)-1)] for p in path])
        new_gyrp_z_test = np.array([gyro_z_test[min(p[1],len(imu_test)-1)] for p in path])
        error = np.linalg.norm(new_gyro_z_0 - new_gyrp_z_test, ord=1)
        print("dtw distance", distance, "error", error, "path length", len(path))
        self.mapped_imu_lengths.append(len(path))

        

        # fig,ax = plt.subplots(3,1)
        # plt.suptitle("Gyroscope Data")
        # ax[0].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["gyro_x"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[0].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["gyro_x"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[0].legend()
        # ax[1].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["gyro_y"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[1].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["gyro_y"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[1].legend()
        # ax[2].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["gyro_z"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[2].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["gyro_z"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[2].legend()

        # fig,ax = plt.subplots(3,1)
        # plt.suptitle("Accelerometer Data")
        # ax[0].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["acc_x"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[0].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["acc_x"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[0].legend()
        # ax[1].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["acc_y"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[1].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["acc_y"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[1].legend()
        # ax[2].plot(np.arange(len(new_gyro_z_0)), np.array([imu_0["acc_z"].iloc[min(p[0],len(imu_0)-1)] for p in path]), label="0")
        # ax[2].plot(np.arange(len(new_gyro_z_0)), np.array([imu_test["acc_z"].iloc[min(p[1],len(imu_test)-1)] for p in path]), label="test")
        # ax[2].legend()

        mapped_df = pd.DataFrame(data = {"time": np.arange(len(new_gyro_z_0)),
                                "gyro_x": np.array([imu_test["gyro_x"].iloc[min(p[1],len(imu_test)-1)] for p in path]),
                                "gyro_y": np.array([imu_test["gyro_y"].iloc[min(p[1],len(imu_test)-1)] for p in path]),
                                "gyro_z": np.array([imu_test["gyro_z"].iloc[min(p[1],len(imu_test)-1)] for p in path]),
                                "acc_x": np.array([imu_test["acc_x"].iloc[min(p[1],len(imu_test)-1)] for p in path]),
                                "acc_y": np.array([imu_test["acc_y"].iloc[min(p[1],len(imu_test)-1)] for p in path]),
                                "acc_z": np.array([imu_test["acc_z"].iloc[min(p[1],len(imu_test)-1)] for p in path])})
        self.imu_logs_mapped[station][device_dir] = mapped_df

        if len(path) > len(self.imu_logs_mapped[station][self.device_dir_order[station][0]]):
            mapped_0_df = pd.DataFrame(data = {"time": np.arange(len(new_gyro_z_0)),
                                    "gyro_x": np.array([imu_0["gyro_x"].iloc[min(p[0],len(imu_0)-1)] for p in path]),
                                    "gyro_y": np.array([imu_0["gyro_y"].iloc[min(p[0],len(imu_0)-1)] for p in path]),
                                    "gyro_z": np.array([imu_0["gyro_z"].iloc[min(p[0],len(imu_0)-1)] for p in path]),
                                    "acc_x": np.array([imu_0["acc_x"].iloc[min(p[0],len(imu_0)-1)] for p in path]),
                                    "acc_y": np.array([imu_0["acc_y"].iloc[min(p[0],len(imu_0)-1)] for p in path]),
                                    "acc_z": np.array([imu_0["acc_z"].iloc[min(p[0],len(imu_0)-1)] for p in path])})
            self.imu_logs_mapped[station][self.device_dir_order[station][0]] = mapped_0_df



        # new_gyro_z_0_time = np.array([gyro_z_0_time[min(p[0],len(imu_0)-1)] for p in path])
        # new_gyro_z_test_time = np.array([gyro_z_test_time[min(p[1],len(imu_test)-1)] for p in path])
        # f_time = interpolate.interp1d(new_gyro_z_test_time, new_gyro_z_0_time, kind='linear', fill_value='extrapolate')

        # full_gyro_z_0 = self.imu_logs[station][self.device_dir_order[station][0]]["gyro_z"].to_numpy()
        # full_gyro_z_0_time = self.imu_logs[station][self.device_dir_order[station][0]]["time"].to_numpy()
        # full_gyro_z_test = self.imu_logs[station][device_dir]["gyro_z"].to_numpy()
        # full_gyro_z_test_time = self.imu_logs[station][self.device_dir_order[station][0]]["time"].to_numpy()
        # adjusted_gyro_z_test_time = f_time(full_gyro_z_test_time)

        # plt.figure()
        # plt.plot(full_gyro_z_0_time, full_gyro_z_0, label="gyro_z_0")
        # plt.plot(full_gyro_z_test_time, full_gyro_z_test, label="gyro_z_test")
        # plt.plot(adjusted_gyro_z_test_time, full_gyro_z_test, label="gyro_z_test_adjusted")
        # plt.legend()


        # plt.show()

        # print(gyro_z_0.shape)
        # print(gyro_z_0)
        # gyro_z_0 = np.where(np.abs(gyro_z_0) > 5., gyro_z_0, 0.)
        # for roll_idx in roll_indexes:
        #     gyro_z_test = np.roll(self.imu_logs[station][device_dir]["gyro_z"].to_numpy(), roll_idx)
        #     gyro_z_test = np.where(np.abs(gyro_z_test) > 5., gyro_z_test, 0.)
        #     l2_diffs.append(np.linalg.norm(gyro_z_0 - gyro_z_test))
        
        # roll_idx = roll_indexes[np.argmin(l2_diffs)]
        # print("min l2 diff", np.min(l2_diffs), "at idx: ", np.argmin(l2_diffs), "roll_idx", roll_idx, "max l2 diff", np.max(l2_diffs))
        # plt.figure()
        # plt.plot(np.arange(len(gyro_z_0)), self.imu_logs[station][self.device_dir_order[station][0]]["gyro_z"].to_numpy(), label="gyro_z_0")
        # plt.plot(np.arange(len(gyro_z_0)), 
        #          np.roll(self.imu_logs[station][device_dir]["gyro_z"].to_numpy(), roll_idx), 
        #          label="gyro_z_test_rolled")
        # # plt.plot(np.arange(len(gyro_z_0)), 
        # #          self.imu_logs[station][device_dir]["gyro_z"].to_numpy(), 
        # #          label="gyro_z_test_orig")
        # plt.legend()
        # plt.show()

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

    def plot_imu_values(self, log_imu):
        """Plot accelerometer and gyroscope data.
        
        Parameters
        ----------
        log_imu : pd.DataFrame
            Dataframe containing the imu data.

        ax_accel : matplotlib.axes.Axes
            Axes object into which the accelerometer data shall be plotted.

        ax_gyro : matplotlib.axes.Axes
            Axes object into which the gyroscope data shall be plotted.
        
        """

        for session in log_imu["session"].unique():
            df_temp = log_imu[log_imu["session"]==session].copy()
            df_temp["time"] = pd.to_datetime(df_temp["time"], format="mixed")
            df_temp["acc_total"] = (df_temp["acc_x"]**2 + df_temp["acc_y"]**2 + df_temp["acc_z"]**2)**0.5

            # plot accelerometer data
            df_temp.plot(x="time",y=["acc_x","acc_y","acc_z","acc_total"], title=f"Accelerometer Session:{session}")
            df_temp.plot(x="time",y=["acc_x","acc_y","acc_z","acc_total"], title=f"Accelerometer Session:{session}")

            
            # plot gyroscope data
            # df_temp.plot(x="time",y=["gyro_x","gyro_y","gyro_z"], title=f"Gyroscope Session:{session}", ax=ax_gyro)

    def plot_all_values(self):
        # fig_acc, axes_acc = plt.subplots(3, 1, figsize=(18, 10))
        # plt.suptitle("Accelerometer Data")
        fig_gyro, axes_gyro = plt.subplots(3, 1, figsize=(18, 10))
        plt.suptitle("Gyroscope Data")
        fig_gyro, axes_gyro_mapped = plt.subplots(3, 1, figsize=(18, 10))
        plt.suptitle("Mapped Gyroscope Data")
        # fig_mag, axes_mag = plt.subplots(3, 1, figsize=(18, 10))
        # plt.suptitle("Magnetometer Data")
        for station in self.imu_logs.keys():
            for device_dir in self.device_dir_order[station]:
                if self.imu_logs[station][device_dir] is not None:
                    # self.imu_logs[station][device_dir].plot(x="time",y="acc_x", ax=axes_acc[0])
                    # self.imu_logs[station][device_dir].plot(x="time",y="acc_y", ax=axes_acc[1])
                    # self.imu_logs[station][device_dir].plot(x="time",y="acc_z", ax=axes_acc[2])
                    self.imu_logs[station][device_dir].plot(x="time",y="gyro_x", ax=axes_gyro[0])
                    self.imu_logs[station][device_dir].plot(x="time",y="gyro_y", ax=axes_gyro[1])
                    self.imu_logs[station][device_dir].plot(x="time",y="gyro_z", ax=axes_gyro[2])
                    self.imu_logs_mapped[station][device_dir].plot(x="time",y="gyro_x", ax=axes_gyro_mapped[0])
                    self.imu_logs_mapped[station][device_dir].plot(x="time",y="gyro_y", ax=axes_gyro_mapped[1])
                    self.imu_logs_mapped[station][device_dir].plot(x="time",y="gyro_z", ax=axes_gyro_mapped[2])
                    # self.mag_logs[station][device_dir].plot(x="system_time",y="mag_x", ax=axes_mag[0])
                    # self.mag_logs[station][device_dir].plot(x="system_time",y="mag_y", ax=axes_mag[1])
                    # self.mag_logs[station][device_dir].plot(x="system_time",y="mag_z", ax=axes_mag[2])
                else:
                    print(f"Station: {station}, Device: {device_dir}", "no imu data")

"""
https://github.com/slaypni/fastdtw/blob/master/fastdtw/fastdtw.py

The MIT License (MIT)

Copyright (c) 2015 Kazuaki Tanida

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


def fastdtw(x, y, radius=1, dist=None):
    ''' return the approximate distance between 2 time series with O(N)
        time and memory complexity

        Parameters
        ----------
        x : array_like
            input array 1
        y : array_like
            input array 2
        radius : int
            size of neighborhood when expanding the path. A higher value will
            increase the accuracy of the calculation but also increase time
            and memory consumption. A radius equal to the size of x and y will
            yield an exact dynamic time warping calculation.
        dist : function or int
            The method for calculating the distance between x[i] and y[j]. If
            dist is an int of value p > 0, then the p-norm will be used. If
            dist is a function then dist(x[i], y[j]) will be used. If dist is
            None then abs(x[i] - y[j]) will be used.

        Returns
        -------
        distance : float
            the approximate distance between the 2 time series
        path : list
            list of indexes for the inputs x and y

        Examples
        --------
        >>> import numpy as np
        >>> import fastdtw
        >>> x = np.array([1, 2, 3, 4, 5], dtype='float')
        >>> y = np.array([2, 3, 4], dtype='float')
        >>> fastdtw.fastdtw(x, y)
        (2.0, [(0, 0), (1, 0), (2, 1), (3, 2), (4, 2)])
    '''
    x, y, dist = __prep_inputs(x, y, dist)
    return __fastdtw(x, y, radius, dist)


def __difference(a, b):
    return abs(a - b)


def __norm(p):
    return lambda a, b: np.linalg.norm(np.atleast_1d(a) - np.atleast_1d(b), p)


def __fastdtw(x, y, radius, dist):
    min_time_size = radius + 2

    if len(x) < min_time_size or len(y) < min_time_size:
        return dtw(x, y, dist=dist)

    x_shrinked = __reduce_by_half(x)
    y_shrinked = __reduce_by_half(y)
    distance, path = \
        __fastdtw(x_shrinked, y_shrinked, radius=radius, dist=dist)
    window = __expand_window(path, len(x), len(y), radius)
    return __dtw(x, y, window, dist=dist)


def __prep_inputs(x, y, dist):
    x = np.asanyarray(x, dtype='float')
    y = np.asanyarray(y, dtype='float')

    if x.ndim == y.ndim > 1 and x.shape[1] != y.shape[1]:
        raise ValueError('second dimension of x and y must be the same')
    if isinstance(dist, numbers.Number) and dist <= 0:
        raise ValueError('dist cannot be a negative integer')

    if dist is None:
        if x.ndim == 1:
            dist = __difference
        else: 
            dist = __norm(p=1)
    elif isinstance(dist, numbers.Number):
        dist = __norm(p=dist)

    return x, y, dist


def dtw(x, y, dist=None):
    ''' return the distance between 2 time series without approximation

        Parameters
        ----------
        x : array_like
            input array 1
        y : array_like
            input array 2
        dist : function or int
            The method for calculating the distance between x[i] and y[j]. If
            dist is an int of value p > 0, then the p-norm will be used. If
            dist is a function then dist(x[i], y[j]) will be used. If dist is
            None then abs(x[i] - y[j]) will be used.

        Returns
        -------
        distance : float
            the approximate distance between the 2 time series
        path : list
            list of indexes for the inputs x and y

        Examples
        --------
        >>> import numpy as np
        >>> import fastdtw
        >>> x = np.array([1, 2, 3, 4, 5], dtype='float')
        >>> y = np.array([2, 3, 4], dtype='float')
        >>> fastdtw.dtw(x, y)
        (2.0, [(0, 0), (1, 0), (2, 1), (3, 2), (4, 2)])
    '''
    x, y, dist = __prep_inputs(x, y, dist)
    return __dtw(x, y, None, dist)


def __dtw(x, y, window, dist):
    len_x, len_y = len(x), len(y)
    if window is None:
        window = [(i, j) for i in range(len_x) for j in range(len_y)]
    window = ((i + 1, j + 1) for i, j in window)
    D = defaultdict(lambda: (float('inf'),))
    D[0, 0] = (0, 0, 0)
    for i, j in window:
        dt = dist(x[i-1], y[j-1])
        D[i, j] = min((D[i-1, j][0]+dt, i-1, j), (D[i, j-1][0]+dt, i, j-1),
                      (D[i-1, j-1][0]+dt, i-1, j-1), key=lambda a: a[0])
    path = []
    i, j = len_x, len_y
    while not (i == j == 0):
        path.append((i-1, j-1))
        i, j = D[i, j][1], D[i, j][2]
    path.reverse()
    return (D[len_x, len_y][0], path)


def __reduce_by_half(x):
    return [(x[i] + x[1+i]) / 2 for i in range(0, len(x) - len(x) % 2, 2)]


def __expand_window(path, len_x, len_y, radius):
    path_ = set(path)
    for i, j in path:
        for a, b in ((i + a, j + b)
                     for a in range(-radius, radius+1)
                     for b in range(-radius, radius+1)):
            path_.add((a, b))

    window_ = set()
    for i, j in path_:
        for a, b in ((i * 2, j * 2), (i * 2, j * 2 + 1),
                     (i * 2 + 1, j * 2), (i * 2 + 1, j * 2 + 1)):
            window_.add((a, b))

    window = []
    start_j = 0
    for i in range(0, len_x):
        new_start_j = None
        for j in range(start_j, len_y):
            if (i, j) in window_:
                window.append((i, j))
                if new_start_j is None:
                    new_start_j = j
            elif new_start_j is not None:
                break
        start_j = new_start_j

    return window

if __name__ == "__main__":
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    avg = AverageTest(dataset_path)