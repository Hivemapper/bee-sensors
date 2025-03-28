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
from imu_mag_qa_dba import QaImuMagDBA
import matplotlib.pyplot as plt

def main():

    data_cols = ["acc_x", "acc_y", "acc_z", "gyro_x", "gyro_y", "gyro_z", "mag_x", "mag_y", "mag_z"]
    l2_error, zero_mean_bias = process_dataset(data_cols)
    plot_l2_error(l2_error, data_cols)
    plot_zero_mean_bias(zero_mean_bias, data_cols)
    plt.show()

def process_dataset(data_cols):

    l2_error = {}
    zero_mean_bias = {}

    # run for each test log
    dataset_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/"
    for station in sorted(os.listdir(dataset_path)):
            print(station)
            l2_error[station] = pd.DataFrame(columns=data_cols)
            zero_mean_bias[station] = pd.DataFrame(columns=data_cols)

            directories = [d for d in os.listdir(os.path.join(dataset_path, station)) if os.path.isdir(os.path.join(dataset_path, station, d))]
            for d_idx, device_dir in enumerate(sorted(directories, key=extract_timestamp)):
                db_dir = os.path.join(dataset_path, station, device_dir)
                db_path = next((os.path.join(db_dir,x) for x in os.listdir(db_dir) if x.endswith(".db") and "sensors" in x), None)
                if db_path is None or "fail" in db_path:
                    continue
                # station = db_path.split("/")[-3]
                # station_reference_path = os.path.join(os.path.dirname(db_path), "..", f"dba_centers_{station.lower()}.csv")
                station_reference_path = "/home/derekhive/datasets/IMU-Data_2025-03-18/Station1/dba_centers_station1.csv"
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
                
                l2_error_new = pd.DataFrame(avg.test_metrics["l2_error"], index=[0])
                zero_mean_bias_new = pd.DataFrame(avg.test_metrics["zero_mean_bias"], index=[0])
                if d_idx == 0:
                    l2_error[station] = l2_error_new
                    zero_mean_bias[station] = zero_mean_bias_new
                else:
                    l2_error[station] = pd.concat([l2_error[station], l2_error_new],
                                                ignore_index=True)
                    zero_mean_bias[station] = pd.concat([zero_mean_bias[station], zero_mean_bias_new],
                                                        ignore_index=True)
                # if d_idx >= 4:
                #     break


    return l2_error, zero_mean_bias

def plot_l2_error(l2_error, data_cols):
    fig, axs = plt.subplots(3, 3, figsize=(18, 10))
    axs = axs.flatten()
    for i, col in enumerate(data_cols):
        for j, station in enumerate(l2_error.keys()):
            axs[i].boxplot(l2_error[station][col], positions=[j+1], label=station)
            if j == 0:
                axs[i].set_title(f"{col}")
                axs[i].set_xlabel("Test Log")
                axs[i].set_ylabel("L2 Error")
    plt.suptitle("L2 Error")
    fig.tight_layout()

def plot_zero_mean_bias(zero_mean_bias, data_cols):
    fig, axs = plt.subplots(3, 3, figsize=(18, 10))
    axs = axs.flatten()
    for i, col in enumerate(data_cols):
        for j, station in enumerate(zero_mean_bias.keys()):
            axs[i].boxplot(zero_mean_bias[station][col], positions=[j+1], label=station)
            if j == 0:
                axs[i].set_title(f"{col}")
                axs[i].set_xlabel("Test Log")
                axs[i].set_ylabel("Zero Mean Bias")
    plt.suptitle("Zero Mean Bias")
    fig.tight_layout()

def extract_timestamp(directory_name):
    timestamp_str = directory_name.rsplit("_", 1)[-1]
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

if __name__ == "__main__":
    main()