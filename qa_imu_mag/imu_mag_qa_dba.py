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
from dtaidistance import dtw
from dtaidistance import dtw_visualisation as dtwvis
import matplotlib.pyplot as plt

class QaImuMagDBA():
    """QA test comparing IMU/Mag data against the average for the robot arm.
    
    Parameters
    ----------
    db_path : str
        Path to the sesnors.db file containing IMU and magnetometer data.
    sn : str, optional
        Serial number of the device. This is used for naming output files.
        If not provided, an empty string is used by default.
    
    """
    def __init__(self, db_path, log_dir, station_reference_file, verbose=False):
        self.db_path = db_path
        self.log_dir = log_dir
        self.station_reference_file = station_reference_file
        self.verbose = verbose
        
        self.mag_log = None
        self.imu_log = None
        self.station_reference = None
        self.test_data = None

        self.imu_cols = ["acc_x", "acc_y", "acc_z", "gyro_x", "gyro_y", "gyro_z"]
        self.mag_cols = ["mag_x", "mag_y", "mag_z"]
        self.downsampled_data = {c : None for c in self.imu_cols + self.mag_cols}
        self.test_results = {c : False for c in self.imu_cols + self.mag_cols}
        self.test_metrics = {}

        # global parameters
        self.debug_plots = False
        self.test_thresholds = {
                                "acc_x" : 0.2,
                                "acc_y" : 0.1,
                                "acc_z" : 0.15,
                                "gyro_x" : 12.0,
                                "gyro_y" : 2.5,
                                "gyro_z" : 30.0,
                                "mag_x" : 350.0,
                                "mag_y" : 175.0,
                                "mag_z" : 250.0,
                               }
        self.load_database()
        self.load_station_reference()


    def run_test(self):
        """Run the QA test.

        This method runs the test and returns True if the test passes,
        otherwise it returns False.

        Returns
        -------
        bool
            True if the test passes, False otherwise.
        
        """

        self.compute_dtw()

        # check L2 error against thresholds
        test_pass = True
        for col in self.imu_cols + self.mag_cols:
            if self.test_metrics["l2_error"].get(col, np.inf) < self.test_thresholds[col]:
                self.test_results[col] = True
            else:
                self.test_results[col] = False
                test_pass = False

        self.write_results()

        return test_pass

    def compute_dtw(self):

        self.test_metrics["l2_error"] = {}
        for col in self.imu_cols + self.mag_cols:
            if self.verbose:
                print(f"Computing DTW for {col}")
            test_series = self.test_data[col].values
            test_series = test_series[~np.isnan(test_series)]
            ref_series = self.station_reference[col].values
            ref_series = ref_series[~np.isnan(ref_series)]
            
            path = dtw.warping_path(ref_series, test_series)
            
            # use path to make time synchroized series
            new_ref_series = np.array([ref_series[i] for i, _ in path])
            new_test_series = np.array([test_series[j] for _, j in path])
            self.test_metrics["l2_error"][col] = np.linalg.norm(new_ref_series - new_test_series)

            if self.debug_plots:
                dtwvis.plot_warping(ref_series, test_series, path, filename=f"{self.sn}_warp_{col}.png")
                plt.plot(new_ref_series, label="Reference")
                plt.plot(new_test_series, label="Test")
                plt.legend()
                plt.savefig(f"{self.sn}_plot_{col}.png")
                plt.close()

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
        self.test_data = pd.DataFrame(dict([(key, pd.Series(value)) for key, value in self.downsampled_data.items()]) )

    def load_station_reference(self):
        self.station_reference = pd.read_csv(self.station_reference_file)

    def downsample(self, col):
        if col in self.mag_cols:
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

    def write_results(self):
        filename = os.path.join(self.log_dir,f"imu_mag_qa_dba_results.log")
        with open(filename, "w") as f:
            for col, bias in self.test_metrics["zero_mean_bias"].items():
                msg = f"Zero mean bias {col}: {bias}"
                f.write(msg + "\n")
                print(msg)
            for col, error in self.test_metrics["l2_error"].items():
                msg = f"L2 error {col}: {error}"
                f.write(msg + "\n")
                print(msg)
            for col in self.mag_cols + self.imu_cols:
                if self.test_results[col]:
                    msg = f"[PASS] {col} similarity check."
                else:
                    msg = f"[FAIL] {col} similarity check."
                f.write(msg + "\n")
                print(msg)            

def extract_timestamp(directory_name):
    timestamp_str = directory_name.rsplit("_", 1)[-1]
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

if __name__ == "__main__":
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    for station in sorted(os.listdir(dataset_path)):
            print(station)
            directories = [d for d in os.listdir(os.path.join(dataset_path, station)) if os.path.isdir(os.path.join(dataset_path, station, d))]
            for d_idx, device_dir in enumerate(sorted(directories, key=extract_timestamp)):
                db_dir = os.path.join(dataset_path, station, device_dir)
                db_path = next((os.path.join(db_dir,x) for x in os.listdir(db_dir) if x.endswith(".db") and "sensors" in x), None)
                if "fail" in db_path:
                    continue
                station = db_path.split("/")[-3]
                station_reference_path = os.path.join(os.path.dirname(db_path), "..", f"dba_centers_{station.lower()}.csv")
                log_dir = os.path.dirname(db_path)
                print(f"Processing {db_path}")

                avg = QaImuMagDBA(db_path,
                                  log_dir,
                                  station_reference_path,
                                  verbose=True)
                if avg.run_test():
                    print(f"[PASS] final")
                else:
                    print(f"[FAIL] final")
                if d_idx > 5:
                    break
            break